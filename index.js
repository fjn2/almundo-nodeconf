const fs = require('fs');
const cluster = require('cluster');
const INPUT_FILE_NAME = 'dataset.csv';
const OUTPUT_FILE_NAME = 'combos.csv';

let comboAmount = 0;

if (cluster.isMaster) {
  function writeFileInit() {
    fileWriteSteam = fs.createWriteStream(OUTPUT_FILE_NAME);
    process.on('exit', () => {
      fileWriteSteam.end();
    });
  }
  function writeFile(item, count) {
    if (comboAmount + count <= 40000) {
      fileWriteSteam.write(item);
    } else {
      let penddingItems = 40000 - comboAmount;
      let newItems = item.split('\n');
      for (let i = 0; i < penddingItems; i++) {
        fileWriteSteam.write(newItems[i] + '\n');
      }
    }
    comboAmount += count;
  }

  writeFileInit();

  for (let i = 0; i < 2; i++) {
    const worker = cluster.fork({PAR: i});

    worker.on('message', (msg) => {
      switch(msg.code) {
        case 'write':
          //console.log('writeFile');
          writeFile(msg.value, msg.count);
          break;
        default:
          console.log('meesage from worker', msg);
          break;
      }
    });

    worker.on('disconnect', () => {
      console.log('workerDisconect');
    });
    cluster.on('exit', (worker, code, signal) => {
      console.log(`worker ${worker.process.pid} died`);
    });
  }
} else {
  initializeWorker();
}

async function initializeWorker() {
  // negrada
  var findOne = false;

  const CHUNK_SIZE = 100000; // lines per read
  const PRICE_LIMIT = 30000;
  const BUCKET_NUMBER = 30000; // this is from 0 to 30,000  (i.e. 500 -> 30000/500 -> $60 each bucket)
  const TOTAL_COMBO = 40000;

  const bucketSize = PRICE_LIMIT / BUCKET_NUMBER;

  let fileWriteSteam;

  function* readFile() {
    const s = fs.createReadStream(INPUT_FILE_NAME);
    const lines = [];
    let reading = true;
    let rest = ''; // this variable is used to complete the last line of the chunk in the followin read
    s.on('data', (chunkB) => {
      let chunk = chunkB + '';
      const newLines = chunk.split('\n');

      // complete the first line with the rest
      newLines[0] = rest + newLines[0];

      // store the last line
      rest = newLines.splice(-1);

      lines.push(...newLines);
    });
    s.on('end', () => {

      process.send({
        code:'message',
        value: process.env.PAR + ' stream finish ' + lines.length
      });
      reading = false;
    });
    while (lines.length > 0 || reading ) {
      yield lines.splice(0, CHUNK_SIZE);
    }
  }

  let linesToWrite = [];

  function generateModel() {
    // for performance reasons
    const data = {
      VUELO: {},
      HOTEL: {}
    }
    return data;
  }
  function addToBucket(data, city, type, itemId, cost) {
    const bucketPosition = Math.ceil(cost / bucketSize);
    const machingId = lookForMaching(data, city, type, itemId, bucketPosition);

    if(!machingId) {
      // the items has not good matching yet

      // create the bucket if doesn't exist
      data[type][city][bucketPosition] = data[type][city][bucketPosition] || [];
      data[type][city][bucketPosition].push(itemId);
    } else {
      // send to the write queue
      if (type === 'VUELO') {
        sendWriteOrder(itemId, machingId);
      } else {
        sendWriteOrder(machingId, itemId);
      }
    }
  }

  function lookForMaching(data, city, type, itemId, bucketPosition) {
    const bucketCandidatePosition = BUCKET_NUMBER - bucketPosition;

    const oppositiveSite = data[type] == data.VUELO ? data.HOTEL : data.VUELO;

    let candidate;

    if (oppositiveSite[city] &&
      oppositiveSite[city][bucketCandidatePosition] &&
      oppositiveSite[city][bucketCandidatePosition].length) {
      // get the last element of candidate bucket
      candidate = oppositiveSite[city][bucketCandidatePosition].splice(-1);
    }

    return candidate;
  }

  function sendWriteOrder(flightId, hotelId) {
    linesToWrite.push(flightId + ',' + hotelId + '\n');
    if (linesToWrite.length > 1000) {
      process.send({
        code:'write',
        value: linesToWrite.join(''),
        count: linesToWrite.length
      });
      linesToWrite.length = 0;
    }
  }
  function flushLines() {
    process.send({
      code:'write',
      value: linesToWrite.join(''),
      count: linesToWrite.length
    });
    linesToWrite.length = 0;
  }

  async function generateSecondMacching(data, bucketError, citiesToSearch) {
    return new Promise((resolve) => {
      const flightKeys = Object.keys(data.VUELO);
      const hotelKeys = Object.keys(data.HOTEL);

      for (let i = 0; i < citiesToSearch.length; i++) {
        const cityFlightBuckets = Object.keys(data.VUELO[citiesToSearch[i]]);
        const cityHotelBuckets = Object.keys(data.HOTEL[citiesToSearch[i]]);

        for (var k = 0; k < cityFlightBuckets.length; k++) {
          // transform with the bucket maching formula
          const bucketComplement = (BUCKET_NUMBER - bucketError) - cityFlightBuckets[k];
          if (
            data.HOTEL[citiesToSearch[i]][bucketComplement] &&
            data.HOTEL[citiesToSearch[i]][bucketComplement].length &&
            data.VUELO[citiesToSearch[i]][cityFlightBuckets[k]] &&
            data.VUELO[citiesToSearch[i]][cityFlightBuckets[k]].length
            ) {
            let hotelId = data.HOTEL[citiesToSearch[i]][bucketComplement].splice(-1);
            let flightId = data.VUELO[citiesToSearch[i]][cityFlightBuckets[k]].splice(-1);

            if (data.HOTEL[citiesToSearch[i]][bucketComplement].length === 0) {
              delete data.HOTEL[citiesToSearch[i]][bucketComplement];
            }
            if (data.VUELO[citiesToSearch[i]][cityFlightBuckets[k]].length === 0) {
              delete data.VUELO[citiesToSearch[i]][cityFlightBuckets[k]];
            }
            //console.log('DING!', flightId, hotelId);
            findOne = true;
            sendWriteOrder(flightId, hotelId);
            // negrada
            resolve();
            return;
          }
        }
      }
      resolve();
    });
  }


  function main() {
    const readFileIterator = readFile();
    const startTime = new Date();
    const data = generateModel();
    let countLines = 0;
    let currentIteration;

    async function processChunk() {
      currentIteration = readFileIterator.next();

      if (!currentIteration.done) {
        newLines = currentIteration.value;

        let currentLine;
        for (let i = 0; i < newLines.length; i++) {
          currentLine = newLines[i].split(','); // itemId itemType cityId cost
          countLines++;
          // filter cities
          if (currentLine[2].charCodeAt(0) % 2 == process.env.PAR) {
            // initialize the bucket for new cities
            data[currentLine[1]][currentLine[2]] = data[currentLine[1]][currentLine[2]] || {};

            addToBucket(data, currentLine[2], currentLine[1], currentLine[0], currentLine[3]);
          }
        }
        setImmediate(processChunk);
      } else {
        flushLines();
        setTimeout(() => {
          process.exit(0);
        });
      }
    }
    processChunk();
  }
  main();
}
