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
	
	const write = function(data) {
		var self = this;
		
		self.emit("data", data.toString() + "\n");  // emit the new data
		self.pause();                               // pause stream
		setTimeout(function() {                     // wait
			self.resume();                          // resume after timeout
		}, timeInterval);
	};
	
	const end = function() {
		output.progress(100);
		console.log("ended");
		this.emit("end");
	};
	
	output.debug("fetching data for %s", dataInId);
	
	request
		.pipe(es.split())                           // split the file into lines
		.pipe(es.through(write,end))                // pass each line through the 'write' function
		.pipe(converter)                            // then pipe to converter
		.on("record_parsed", function(jsonObj) {
			// do work here.
			var singleEntry = jsonObj;
			
			var addressString = "";
			for (var i = 0; i < len; i++) {
				addressString = addressString + singleEntry[addressArray[i]] + ", ";
			}
			
			addressString = addressString + "United Kingdom";
			
			var stringAddress = String(addressString);
			
			var singleData = {"type": "Feature","properties": {}, "geometry": {"type": "Point", "coordinates": []}};
			
			geocoder.geocode(stringAddress, function (err, data) {
				console.log("streaming geo-googleapi data ...");
				
				if (data.status == "OK") {
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
				
				writeStream.write(JSON.stringify(singleData) + "\n");
			}, apiKey);
		});
	
}

var input = require("nqm-databot-utils").input;
input.pipe(databot);
