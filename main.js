/*
 * Created by G on 12/10/2016.
 */

const geocoder = require("geocoder");
const fs = require("fs");
const Converter = require("csvtojson").Converter;
const es = require("event-stream");
const _ = require("lodash");

function databot(input, output, context) {
  output.progress(0);

  if (!input.timeInterval || !input.dataInId || !input.outPath || !input.addressArray || !input.googleApiKey) {
    output.abort("invalid arguments - please supply timeInterval, dataInId, outPath, addressArray, googleApiKey");
  }

  const dataInId = input.dataInId;
  const outPath = input.outPath;
  const addressArray = input.addressArray;
  const apiKey = { key: input.googleApiKey };
  const converter = new Converter({ constructResult: false });
  const api = context.tdxApi;
  const writeStream = fs.createWriteStream(outPath);
  const tempPath = outPath + ".tmp.json";

  ////////////////////////////
  var _countWrite = 0;
  var _countCon = 0;
  var _countGApi = 0;
  var _lineCount = 0;

  const write = function (data) {
    _countWrite++;
    this.emit("data", data.toString() + "\n");  // emit the new data
  };

  const end = function () {
    output.debug("reached CSV input end %d/%d", _countWrite, _countCon);
    this.emit("end");
  };

  // Create a pause-able stream 
  const through = es.through(write, end);

  // Handles a single CSV row of data.
  const parseRow = function (singleEntry) {
    through.pause();

    _countCon++;

    var addressString = "";
    _.forEach(addressArray, (address) => {
      if (singleEntry[address]) {
        addressString = addressString + singleEntry[address] + ", ";
      }
    });

    addressString = addressString + "United Kingdom";

    var singleData = { "type": "Feature", "properties": {}, "geometry": { "type": "Point", "coordinates": [] } };

    geocoder.geocode(addressString, function (err, data) {
      _countGApi++;

      if (err) {
        output.debug("countWrite: " + _countWrite + ", countGApi: " + _countGApi + ", countCon: " + _countCon);
        output.abort("the google api query error: %s ", err);
      } else if (data.status == "OK") {
        // obtaining the most probable location
        var loc = data.results[0].geometry.location;

        singleData.geometry.coordinates = [loc.lng, loc.lat];
        singleEntry.geoStatus = data.status;
      } else {
        //failed to get data
        output.debug("Failed to get google api query data - %s", data.status);
        output.debug("The failed data address is - %s", addressString);

        singleEntry.geoStatus = data.status;// usually: ZERO_RESULTS is given, indicating wrong address format
      }

      singleData.properties = singleEntry;

      writeStream.write(JSON.stringify(singleData) + "\n");

      output.progress(100*_countGApi/_lineCount);

      setTimeout(function () {                      // wait
        through.resume();                         // resume after timeout
      }, input.timeInterval);

    }, apiKey);
  };

  const getLineCount = function(cb) {
    var err;
    var lc = 0;
    
    fs.createReadStream(tempPath)
      .pipe(es.split())
      .on("data", () => {
        lc++;
      })
      .on("end", () => {
        if (err) {
          return;
        }
        cb(null, lc - 1);
      })
      .on("error", (error) => {
        err = error;
        cb(err);
      });
  };

  const tidyUp = function() {
    fs.unlink(tempPath, function(err) {
      if (err) {
        output.error("failed to remove temporary file %s", tempPath);
      }

      output.debug("finished");
      output.progress(100);
    });
  };

  const tempFileReady = function () {
    getLineCount(function(err, lc) {
      _lineCount = lc;
      output.debug("about to process %d rows", lc);

      // Open temp file as readable.
      fs.createReadStream(tempPath)
        .pipe(es.split())                           // split the file into lines
        .pipe(through)                							// pass each line through the "write" function
        .pipe(converter)                            // then pipe to converter
        .on("record_parsed", parseRow)
        .on("end_parsed", tidyUp);
    });
  };

  // Download the source file and store locally during processing. This is necessary as pausing the HTTP
  // response stream seems to cause data to be lost.
  var tempStream = fs.createWriteStream(tempPath);  
  output.debug("fetching data for %s and writing to local file %s", dataInId, tempPath);
  api.getRawFile(dataInId).pipe(tempStream);

  // Wait until the temporary file is written before processing begins.
  tempStream.on("finish", tempFileReady);
}

var input = require("nqm-databot-utils").input;
input.pipe(databot);
