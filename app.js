var express = require("express"),
    app = express(),
    csvWriter = require('csv-write-stream'),
    fs = require("fs");

function writeCSVFile(body) {
    var headers = [],
        extraHeaders = ['topic', 'offset', 'partition'],
        fields = [],
        extraFields = ['topic', 'offset', 'partition'];
    for (var attributename in body[0].value) {
        if (typeof(body[0].value[attributename]) != "object") {
            headers.push(attributename);
            fields.push('value.' + attributename);
        }
    }
    headers = headers.concat(extraHeaders);
    fields = fields.concat(extraFields);
    var writer = csvWriter({
        headers: headers
    });
    writer.pipe(fs.createWriteStream('sample_predicted_data.csv'));
    body.forEach(function(item, itemIndex) {
        data = [];
        fields.forEach(function(fieldName, index) {
            if (eval('item.' + fieldName)) {
                data.push(eval('item.' + fieldName));
            } else {
                data.push('');
            }
        })
        writer.write(data);
    })

    writer.end()
    console.log('CSV write complete');
}

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function parseJSONData(body) {
    var fillFields = ['email', 'display', 'social', 'web', 'catalog', 'search', 'content', 'SMS', 'offer'],
        writeText = "";
    body.forEach(function(item, itemIndex) {
        if (!body[itemIndex].value.predictedType) {
            var dataIndex = getRandomInt(0, 8);
            body[itemIndex].value.predictedType = fillFields[dataIndex];
        }
    })
    writeText = JSON.stringify(body);
    fs.writeFile('parsed_predicted.json', writeText, function(err) {
        if (err) return console.log(err);
        console.log('JSON write complete');
    });
}

function loadData() {
    fs.readFile('sample_predicted_data.json', function(err, data) {
        data = JSON.parse(data);
        data.forEach(function(url, index) {
            data[index].value = JSON.parse(data[index].value);
        })
        writeCSVFile(data);
        parseJSONData(data);
    });
}
loadData();
app.get('/convertData', function(req, res) {
    fs.readFile('sample_predicted_data.json', function(err, data) {
        data = JSON.parse(data);
        var responseData = data[0];
        if (typeof(responseData.value) != 'object') {
            responseData.value = JSON.parse(responseData.value);
        }
        if (err) {
            res.send(err);
        } else {
            res.send(responseData);
        }
    });
})
app.listen(process.env.PORT || 3000);
