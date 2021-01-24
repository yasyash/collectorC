const request = require('node-fetch');
const FormData = require('form-data');

const Stations = require('../models/stations');
const Sensors_data = require('../models/sensors_data');
const Injection = require('../models/injection');
const Equipments = require('../models/equipments');
const Injected = require('../models/injected');

const format = require('node.date-time');
const isEmpty = require('lodash.isempty');
const merge = require('lodash.merge');

var aspapi_codes_inv = {
    "Пыль общая": "P001",
    "PM1": "PM1",
    "PM2.5": "P301",
    "PM10": "P201",
    "NO2": "P005",
    "NO": "P006",
    "NH3": "P019",
    "бензол": "P028",
    "HF": "P030",
    "HCl": "P015",
    "м,п-ксилол": "м,п-ксилол",
    "о-ксилол": "о-ксилол",
    "O3": "P007",
    "H2S": "P008",
    "SO2": "P002",
    "стирол": "P068",
    "толуол": "P071",
    "CO": "P004",
    "фенол": "P010",
    "CH2O": "P022",
    "хлорбензол": "P077",
    "этилбензол": "P083"
};

async function injector() {
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
    _appGlobal++;


    await Injection
        .where('is_present', true)
        .where('id', 2) //for esolated pseudo-thread for each API direction
        .fetchAll()
        .then(_stations => {
            station = _stations.toJSON();

            if (station.length > 0) {

                for (key in station) {
                    let _eot = new Date().getTime() - 86400000; //datetime now - one day in ms
                    let _bot = new Date(station[key].date_time).getTime(); //plus one second
                    let fromTime = new Date(_bot + 1000).format('Y-MM-ddTHH:mm:SS');

                    //console.log("BOT = ", _bot, " EOT = ", _eot);
                    if (_eot > _bot) {
                        fromTime = new Date(_eot).format('Y-MM-ddTHH:mm:SS'); // if time between DB and time now more than 24 hours
                    }



                    const between_date = [fromTime, new Date().format('Y-MM-ddTHH:mm:SS')];
                    //console.log("between  = ", between_date);
                    var _stat = fetch_data(station[key].id, station[key].idd, between_date, station[key].last_time, station[key].uri, station[key].code, station[key].token, station[key].indx, station[key].msg_id, station[key].msg_id_out)

                }
            }
        }).catch(err => {
            console.log('Database connection failed...', err)
            _appGlobal--;
        });


}

async function fetch_data(id, idd, between_date, last_time, uri, code, token, indx, msg_id, msg_id_out) {

    var _ms_id = Number(msg_id);
    var _msg_id_out = Number(msg_id_out);
    var _limit = Number(msg_id) + 1;
    var _go_out = false;//flag for exit from while
    var _conn_status = false; //connection result
    var planty = [];
    var data = [];
    var i = 0;
    // cursors prepare
    await Equipments
        .where('idd', idd)
        .where('is_present', 'true')
        .fetchAll()
        .then(_equipments => {

            equipments = _equipments.toJSON();
        }).catch(err => {
            console.log('Database fetching data failed...', err)
            _appGlobal--;
        });

    //if not records detection
    await Sensors_data
        .query('whereBetween', 'date_time', between_date)
        .orderBy('date_time', 'asc')
        .fetchAll()
        .then(_planty => {
            planty = _planty.toJSON();
            if (planty.length == 0)
                _go_out = true;
            else
                between_date[0] = planty[0].date_time; //shift time begin to first record time

        });
    //console.log("BETWEEN NEW", between_date);
    //console.log("total records is ", planty.length);

    //begin while
    while (!_go_out) {
        //console.log("iteration = ", i)
        //if (_go_out) break;
        var measure_time;


        if (equipments.length > 0) {

            var params = {};
            var marker = {};


            var time_frame = [new Date(new Date(between_date[0]).getTime() + 60000 * i).format('Y-MM-ddTHH:mm:SS'), new Date((new Date(between_date[0]).getTime() + 60000 * (i + 1) - 1000)).format('Y-MM-ddTHH:mm:SS')];
            if (new Date(between_date[1]).getTime() < new Date((new Date(between_date[0]).getTime() + 60000 * (i + 1))))
                _go_out = true;
            //if (_go_out) break;
            //console.log('data = ', planty);

            for (_key in equipments) {
                marker = {};
                //console.log("mKEYS = ", equipments[_key]);

                await Sensors_data
                    .query('whereBetween', 'date_time', time_frame)
                    .where('serialnum', equipments[_key].serialnum)
                    .query({
                        andWhereRaw: ("typemeasure != 'Напряжение мин.' and typemeasure != 'Напряжение макс.' and typemeasure != 'Темп. сенсор ИБП' ")
                    })
                    .orderBy('date_time', 'asc')
                    .fetchAll()
                    .then(_data => {
                        data = _data.toJSON();

                        //console.log("new frame ----", time_frame)
                        if (data.length > 0) {//create a pouch with measurements

                            var pouch = {};

                            for (index in data) {

                                pouch = ({
                                    date_time: new Date(data[index].date_time).format('Y-MM-ddTHH:mm:SS') + '+' + '0' + new Date().getTimezoneOffset() / (-60),
                                    unit: equipments[_key].unit_name,
                                    measure: data[index].measure.toFixed(3)
                                });

                                measure_time = new Date(data[index].date_time).format('Y-MM-ddTHH:mm:SS') + '+' + '0' + new Date().getTimezoneOffset() / (-60);

                            }
                            var name = "";

                            if (aspapi_codes_inv[data[index].typemeasure]) {
                                name = String(aspapi_codes_inv[data[index].typemeasure]);

                            }
                            else {
                                name = String(data[index].typemeasure);

                            }


                            if (!isEmpty(pouch)) {
                                marker[name] = pouch;
                            }


                            if (!isEmpty(marker))
                                merge(params, marker);



                        }

                    }).catch(err => {
                        console.log('Database fetching data failed...', err)
                        _appGlobal--;
                    });


            }

            if (!isEmpty(params)) {
                _msg_id_out++;

                console.log("# msg ----", _msg_id_out);
                
                console.log('params = ', JSON.stringify({
                    "token": token,
                    "message": _msg_id_out, //incremented field
                    "locality": indx,
                    "object": code,
                    "date_time": between_date[1],
                    "params": params
                }));
                const form = new FormData();

                form.append("data", JSON.stringify({
                    "token": token,
                    "message": _msg_id_out,
                    "locality": indx,
                    "object": code,
                    "date_time": between_date[1],
                    "params": params
                }));

                var options = {
                    headers: form.getHeaders(),
                    method: 'POST',
                    body: form
                };



                try {
                    const r = await request(uri, options);
                    const result = await r.json();

                    //console.log("status = ", result.success);
                    _conn_status = true;


                    if (result.success) {

                        let process_time_frame = time_frame[1];

                        if (_ms_id > 0) {
                            _ms_id--;

                            let _time = time_frame[0];
                            if (new Date(time_frame[1]).getTime() < new Date(between_date[1]).getTime()) {
                                _time = time_frame[1];
                            }
                            _time = new Date(new Date(_time).getTime() + 1000).format('Y-MM-ddTHH:mm:SS');
                            injection_update_all_time(id, _time, between_date[1], _ms_id, result.message);

                        } else {
                            //if message line is empty but time frame exist
                            let _time = time_frame[1];
                            detect_data(time_frame[1], between_date[1]).then(_out => {
                                if (_out > 0) {
                                    _limit++;
                                    //console.log("msg_id = 0 but data exist = ", _limit);

                                } else {
                                    _time = between_date[1];
                                    _go_out = true;
                                }
                                if (time_frame[1] > between_date[1]) {

                                    _time = time_frame[0];

                                }
                                //if line is out
                                injection_update_all_time(id, _time, between_date[1], 0, result.message);


                            });


                        }
                        injected_table_ins(result.message, measure_time, uri, result.transaction, idd, result.message);


                    }
                    else {

                        let process_time_frame = new Date((new Date(between_date[1]).getTime() - 86400000)).format('Y-MM-ddTHH:mm:SS'); //value that 24 hours ago from now

                        if (_ms_id < 1440) {

                            _ms_id++;

                            //console.log("_ms_id = ", _ms_id);
                            if (process_time_frame > between_date[0]) {//if less than 1440 measures but time is more than 24 hours

                                injection_update_all_time(id, process_time_frame, process_time_frame, _ms_id, -1)
                                    .then(result => {

                                        _go_out = true;
                                    });

                            } else {
                                injection_update_msg(id, _ms_id)
                                    .then(result => {

                                        _go_out = true;
                                    });
                            }
                        } else {
                            //console.log("process time frame = ", process_time_frame);

                            injection_update_time(id, process_time_frame).then(result => {

                                _go_out = true;
                            })
                        }
                    }


                    // end of try

                } catch (error) {
                    console.log("err is ", error)
                    _conn_status = false;
                    _go_out = true;

                    let process_time_frame = new Date((new Date(between_date[1]).getTime() - 86400000)).format('Y-MM-ddTHH:mm:SS'); //value that 24 hours ago from now

                    if (_ms_id < 1440) {

                        _ms_id++;

                        //console.log("_ms_id = ", _ms_id);
                        if (process_time_frame > between_date[0]) {//if less than 1440 measures but time is more than 24 hours

                            injection_update_all_time(id, process_time_frame, process_time_frame, _ms_id, -1)
                                .then(result => {

                                    _go_out = true;
                                });

                        } else {
                            injection_update_msg(id, _ms_id)
                                .then(result => {

                                    _go_out = true;
                                });
                        }
                    } else {
                        //console.log("process time frame = ", process_time_frame);

                        injection_update_time(id, process_time_frame).then(result => {

                            _go_out = true;
                        })
                    }
                }


            } else {

                //if message line is empty but time frame exist
                detect_data(time_frame[1], between_date[1]).then(_out => {
                    if ((_out == 0)) { //if previous connection is ok
                        _go_out = true;
                        _ms_id = 0;
                        // console.log("_out", _conn_status)
                        //if line is out
                        if (_conn_status)
                            injection_update_all_time(id, between_date[1], between_date[1], 0, -1)
                                .then(result => {
                                    console.log('Emty results');
                                });
                    }
                });

            }
            // end rquest

        }

        //   console.log("i = ", i)
        //  if (i > 100) _go_out = true;
        i++;
    } // end while cycle

    console.log('injection is completed...');
    _appGlobal--;

};

async function detect_data(time_in, time_now) {
    if (new Date(time_in).getTime() < new Date(time_now).getTime()) //detecting to records is exist if msg limit id is emty
    {
        let _period = [time_in, time_now];


        await Sensors_data
            .query('whereBetween', 'date_time', _period)
            .fetchAll()
            .then(__datacur => {
                __data = __datacur.toJSON();

            });

    } else {
        return 0;
    }

    if (__data.length > 0) {
        return 1;
    } else {
        return 0;
    }
}

async function injection_update_all_time(id, _time, last_time, msg_id, _msg_id_out) {

    if (_msg_id_out > -1) {
        await Injection.where({ id: id })
            .save({
                date_time: _time,
                last_time: last_time,
                msg_id: msg_id,
                msg_id_out: _msg_id_out
            }, { patch: true })
            .then(result => {
                //console.log("Message is inserted at ", last_time, " from ", _time);
            }).catch(err => console.log("Update Injection table error...", err));
    } else {
        await Injection.where({ id: id })
            .save({
                date_time: _time,
                last_time: last_time,
                msg_id: msg_id,
            }, { patch: true })
            .then(result => {
                //console.log("Message is inserted at ", last_time, " from ", _time);
            }).catch(err => console.log("Update Injection table error...", err));
    }
}

async function injection_update_msg(id, msg_id) {
    await Injection.where({ id: id })
        .save({

            msg_id: msg_id
        }, { patch: true })
        .then(result => {
            console.log("Message id updated");
        }).catch(err => console.log("Update Injection table error...", err));
}

async function injection_update_time(id, _time) {
    await Injection.where({ id: id })
        .save({
            date_time: _time,

        }, { patch: true })
        .then(result => {
            console.log("Datetime is updated... ");
        }).catch(err => console.log("Update Injection table error...", err));
}

async function injected_table_ins(msg_id, _time, uri, _transaction, idd, _msg_id_out) {
    const _msg_time_ins = new Date().format('Y-MM-ddTHH:mm:SS');
    //console.log("Time is ", _msg_time_ins);
    await Injected.forge({ "date_time": _time, "msg_id": msg_id, "uri": uri, "transaction": _transaction, "msg_time": _msg_time_ins, "idd": idd, "msg_id_out": _msg_id_out }).save()
        .catch(err => console.log("Insert in Injected table error...", err));
}
function try_push() {

    var options = {
        headers: {
            'Content-Type': 'multipart/form-data'
        },
        uri: 'https://asoiza.voeikovmgo.ru/data/rest.php',
        method: 'POST'
        // formData: JSON.stringify( )
    };
    var r = request(options,
        function (err, res, body) {
            console.log("response = ", body);
            if (err) {
                console.log("err = ", err);


            } else {

                var success = res.body.success;
                console.log("status = ", success);

            }

        })

    var form = r.form();
    form.append("data", JSON.stringify({
        "token": "4e0d261a4f1bdc65bb10c5422e0154c1", "message": "1", "locality": "5507340", "object": 2, "date_time": "2020-12-18T02:38:30+03",
        "params":
        {
            "P001":
                { "date_time": "2020-12-1T14:18:51+03", "unit": "мг/м3", "measure": 0.01 }
        }
    }))
}


module.exports = injector;
