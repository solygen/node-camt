//#!/usr/bin/env node
(function () {

    'use strict';

    var fs = require('fs'),
        pt = require('path'),
        csv = require('csv'),
        _ = require('lodash');


    var args = process.argv.slice(2);

    var base = __dirname.replace('/src', '/data/'),
        main = {};

    //create path when not existing
    var ensure = function (path) {
        var sep = pt.sep,
            last = '',
            parts = path.split(sep);
        parts.forEach(function (part) {
            last = last + sep + part;
            if (!fs.existsSync(last))
                fs.mkdirSync(last);
        });
    };

    var hash = function (data) {
        var hashed = {},
            lines = data.rows,
            account = data.account;

        main.columns = data.columns;
        main.accounts = main.accounts || {};
        main.accounts[account] = [];

        //removes headline
        lines.forEach(function (row) {
            var date = row[2],
                parts = date.match(/(\d+)/g),
                year = parts[2],
                key = year + '-' + parts[1];// + '-' + parts[0];

            hashed[year] = hashed[year] || [];
            hashed[year][key] = hashed[year][key] || [];
            hashed[year][key].push(row);
        });

        Object.keys(hashed).forEach(function (year) {
            var yeardata = [], tmp;

            Object.keys(hashed[year]).forEach(function (key) {
                var path = pt.join(base, account, year),
                    file;

                //create path if needed
                ensure(path);

                file = path + key + '.json';

                //union
                if (fs.existsSync(file)) {
                    var existing = fs.readFileSync(file, 'utf8'),
                        data = JSON.parse(existing);
                    hashed[year][key] = (hashed[year][key] || []).concat(data);
                }

                //remove duplicates
                hashed[year][key] = _.uniq(hashed[year][key], function (item) {
                    return JSON.stringify(item);
                });

                //sort
                hashed[year][key].sort(function (a, b) {
                    var partsa = (a[1]).match(/(\d+)/g);
                    var partsb = (b[1]).match(/(\d+)/g);
                    return parseInt((partsa[2] + partsa[1] + partsa[0]), 10) - parseInt((partsb[2] + partsb[1] + partsb[0]), 10);
                });

                //write to file
                yeardata = yeardata.concat(hashed[year][key]);

                fs.writeFileSync(path + key + '.json', JSON.stringify(hashed[year][key], undefined, 2));
            });
            //sum year
            tmp = pt.join(base + account) + pt.sep + (year + '.json');
            main.accounts[account] = main.accounts[account].concat(tmp);
            fs.writeFileSync(tmp, JSON.stringify(yeardata, undefined, 2));
        });

        //TODO:
        fs.writeFileSync(base + 'data.json', JSON.stringify(main, undefined, 2));
    };

    function parseCAMT (file, callback) {
        var lines = [],
            opt = {
                comment: '#',
                delimiter: ';',
                escape: '"'
            };

        //read csv
        csv()
            .from.path(file, opt)
            .transform(function (row) {
                //replace whitespace
                return row.map(function (value) {
                    return value.trim().replace(/\s+/g, ' ');
                });
            })
            .on('record', function (row, index) {
                lines.push(row);
            })
            .on('end', function () {
                console.log(file, ':', lines.length);
                callback.call(this, {
                    columns: lines.shift(),
                    rows: lines,
                    account: (pt.basename(file)).split('-')[1]
                });
            })
            .on('error', function (error){
                console.log(error.message);
            });
    }

    //all csv-camt files
    var files = fs.readdirSync(base + '/source');
    files.forEach(function (file) {
        if (file.split('.')[1] === 'csv')
            parseCAMT(base + '/source/' + file, hash);
    });


}());
