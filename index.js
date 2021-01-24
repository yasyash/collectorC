

const express = require('express');
const bodyParser = require('body-parser');
const query = require('./api/query');
const injector = require('./api/injector');

const { exec } = require('child_process');

const cron = require('node-cron');
const https = require('https');
const fs = require('fs');
const superagent = require('superagent');

const app = express();

app.use(bodyParser.json());
app.use('/query', query);

const options = {
  //key: fs.readFileSync('./keys/chel_key.key'),
  //cert: fs.readFileSync('./keys/asoiza.voeikovmgo.crt')
};


https.createServer({}, app).listen(8383, () => {
  global._appGlobal = 0;

  console.log('Client SSL is started on 8383 port...');
  cron.schedule("* * * * *", () => {
    exec('./api/meteo_data_receive_pg', (error, stdout, stderr) => {
      if (error) {
        console.error(`error: ${error.message}`);
      }
      //console.error(`out: ${stdout}`);

      if (_appGlobal == 0) {
        console.log("injection begin ", _appGlobal);
        injector();
      } else {
        console.log("injection is already running ", _appGlobal);
        if (_appGlobal < 0)
          _appGlobal = 0;
      }
    });
  });

});


