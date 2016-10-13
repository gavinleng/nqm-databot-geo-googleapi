/*
 * Created by G on 12/10/2016.
 */


const geocoder = require('geocoder');
const fs = require("fs");
const Converter = require("csvtojson").Converter;
const es = require("event-stream");

function databot(input, output, context) {
	output.progress(0);
	
	if (!input.timeInterval || !input.dataInId || !input.outPath || !input.addressArray || !input.googleApiKey) {
		output.error("invalid arguments - please supply timeInterval, dataInId, outPath, addressArray, googleApiKey");
		process.exit(1);
	}
	
	const timeInterval = input.timeInterval; // maximum 50 requests per second for google api query
	
	const dataInId = input.dataInId;
	const outPath = input.outPath;
	
	const addressArray = input.addressArray;
	
	const apiKey = {key: input.googleApiKey};
	
	const converter = new Converter({});
	
	const api = context.tdxApi;
	
	const request = api.getRawFile(dataInId);
	
	const writeStream = fs.createWriteStream(outPath);
	
	const len = addressArray.length;
	
	////////////////////////////
	_countWrite =0;
	_countCon =0;
	_countGApi =0;
	
	const write = function(data) {
		var self = this;
		_countWrite ++;
		self.emit("data", data.toString() + "\n");  // emit the new data
		/*self.pause();                               // pause stream
		setTimeout(function() {                     // wait
			self.resume();                          // resume after timeout
		}, timeInterval);*/
	};
	
	const end = function() {
		output.progress(100);
		console.log("ended");
		this.emit("end");
	};
	
	const through = es.through(write,end);
	
	output.debug("fetching data for %s", dataInId);
	
	request
		.pipe(es.split())                           // split the file into lines
		.pipe(through)                // pass each line through the 'write' function
		.pipe(converter)                            // then pipe to converter
		.on("record_parsed", function(jsonObj) {
			// do work here.
			through.pause();
			
			var singleEntry = jsonObj;
			_countCon++;
			var addressString = "";
			for (var i = 0; i < len; i++) {
				if (singleEntry[addressArray[i]]) {
					addressString = addressString + singleEntry[addressArray[i]] + ", ";
				}
			}
			
			addressString = addressString + "United Kingdom";
			
			var stringAddress = String(addressString);
			
			var singleData = {"type": "Feature","properties": {}, "geometry": {"type": "Point", "coordinates": []}};
			
			//////////////////////////////////////////
			if (Math.abs(_countWrite - _countCon) > 2) {
				output.debug("outside of google api: countGApi " + _countGApi + ', countCon ' + _countCon);
				console.log("outside of google api: countGApi " + _countWrite + ', countCon ' + _countCon);
				process.exit(1);
			}
			
			var  apiCount = 0;
			geocoder.geocode(stringAddress, function (err, data) {
				console.log("streaming geo-googleapi data ..." + ++apiCount);
				_countGApi++;
				if (err) {
					output.debug("the google api query error: %s ", err);
					console.log("the google api query error: %s ", err);
					
					output.debug("countWrite: " + _countWrite + ", countGApi: " + _countGApi + ', countCon: ' + _countCon);
					console.log("countWrite: " + _countWrite + ", countGApi: " + _countGApi + ', countCon: ' + _countCon);
					
					process.exit(1);
				}
				else if (data.status == "OK") {
					// obtaining the most probable location
					var loc = data.results[0].geometry.location;
					
					singleData.geometry.coordinates = [loc.lng, loc.lat];
					
					singleEntry.geoStatus = data.status;//"OK"
				} else {
					//failed to get data
					output.debug("Failed to get google api query data - %s", data.status);
					output.debug("The failed data address is - %s", stringAddress);
					console.log(data.status);
					console.log("error: data status is not ok");
					
					singleEntry.geoStatus = data.status;// usually: ZERO_RESULTS is given, indicating wrong address format
				}
				
				singleData.properties = singleEntry;
				
				//////////////////////////////////////////
				if (Math.abs(_countGApi - _countCon) > 2) {
					output.debug("inside of google api: countGApi " + _countGApi + ', countCon ' + _countCon);
					console.log("inside of google api: countGApi" + _countGApi + ', countCon ' + _countCon);
					process.exit(1);
				}
				
				if (_countWrite % 500 == 0) {
					output.debug("countWrite: " + _countWrite + ", countGApi: " + _countGApi + ', countCon: ' + _countCon);
					console.log("countWrite: " + _countWrite + ", countGApi: " + _countGApi + ', countCon: ' + _countCon);
				}
				
				writeStream.write(JSON.stringify(singleData) + "\n");
				
				setTimeout( function () {                      // wait
					through.resume();                         // resume after timeout
				}, timeInterval);
				
			}, apiKey);

		});
	
}

var input = require("nqm-databot-utils").input;
input.pipe(databot);
