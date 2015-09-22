var mosca = require('mosca')
    mysql               = require('mysql'),
    connectionsArray    = [],
    connection          = mysql.createConnection({
        host        : 'localhost',
        user        : 'root',
        password    : 'dbR00T',
        database    : 'sensormonitor',
        port        : 3307
    }),
    POLLING_INTERVAL = 3000,
    pollingTimer = undefined;

var settings = {
http: { port: 3000,
bundle: true,
static: './'
} };
//here we start mosca
var server = new mosca.Server(settings);
server.on('ready', setup);
// fired when the mqtt server is ready
function setup() {
console.log('Mosca server is up and running')
}
// fired whena  client is connected
server.on('clientConnected', function(client) {
console.log('client connected', client.id);
	connectionsArray.push(client.id);
	//getSensorStatus();
	console.log('Conexiones activas', connectionsArray);
});
// fired when a message is received
server.on('published', function(packet, client) {
	//console.log('Published : ', packet.payload);
	if(client){
	console.log('Published: ', packet.topic, packet.payload.toString(), client.id);
	var topic = packet.topic;
	var payload = packet.payload.toString();
	if(topic.indexOf('temperatureUpdate-') === 0){
		var sensorId = topic.substr(18);
                console.log('sensorId', sensorId)
		if(payload === 'graph'){
			console.log('do graph');
		} else{
			console.error('Operacion no definida');
		}
	
	} else {
		if(topic === 'sensorStatus'){
			if(payload === 'getStatus'){
				getSensorStatus();
			}
		} else {
			console.error('Topic no reconocido');
		}
	}
	}
});
// fired when a client subscribes to a topic
server.on('subscribed', function(topic, client) {
	console.log('subscribed : ', topic);
	if(topic === 'sensorStatus'){
		getSensorStatus();
	} else if(topic.indexOf('temperatureUpdate-') === 0){
		console.log('temperatureUpdate', topic);
		if (connectionsArray.length) {
			var sensorId = topic.substr(18);
			console.log('sensorId', sensorId);
		        pollingLoop(sensorId);
		}
	}
});
// fired when a client subscribes to a topic
server.on('unsubscribed', function(topic, client) {
console.log('unsubscribed : ', topic);
});
// fired when a client is disconnecting
server.on('clientDisconnecting', function(client) {
console.log('clientDisconnecting : ', client.id);
});
// fired when a client is disconnected
server.on('clientDisconnected', function(client) {
	console.log('clientDisconnected : ', client.id);
	connectionsArray.splice(connectionsArray.indexOf(client.id), 1);
	console.log('Conexiones activas', connectionsArray);
});



/*
* http://www.gianlucaguarini.com/blog/push-notification-server-streaming-on-a-mysql-database/
* HERE IT IS THE COOL PART
* This function loops on itself since there are sockets connected to the page
* sending the result of the database query after a constant interval
*
*/
var pollingLoop = function (sensorId) {
    
    // Make the database query
    var query = connection.query('SELECT SQL_NO_CACHE value, sensorId, captureDate from sensorValue where sensorId=' + sensorId + ' order by captureDate desc limit 1'),
        temperatures = []; // this array will contain the result of our db query


    // set up the query listeners
    query
    .on('error', function(err) {
        // Handle error, and 'end' event will be emitted after this as well
        console.error( err );
        //updateSockets( err );
        
    })
    .on('result', function( temperature ) {
        // it fills our array looping on each user row inside the db
        console.log(temperature);
	temperatures.push( temperature );
    })
    .on('end',function(){
        // loop on itself only if there are sockets still connected
        if(connectionsArray.length) {
            pollingTimer = setTimeout( function(){ pollingLoop(sensorId); }, POLLING_INTERVAL );

	    var msg = {
		topic: 'temperatureUpdate-' + sensorId,
		payload: JSON.stringify(temperatures)
		};
		console.log(new Date(), msg);

            sendMessage(msg);
		temperatures = [];
		msg = undefined;

		//pollingTimer = setTimeout( pollingLoop(sensorId), POLLING_INTERVAL );
        }
    });

};
var sendMessage = function(mesg){
	server.publish(mesg);
}

var getSensorStatus = function(){
	console.log('getSensorStatus');
	var query = connection.query('select SQL_NO_CACHE name, status from sensor order by id asc');
	var sensors = [];
    // set up the query listeners
    query
    .on('error', function(err) {
        // Handle error, and 'end' event will be emitted after this as well
        console.error( err );

    })
    .on('result', function( sensor ) {
        // it fills our array looping on each user row inside the db
        //console.log(sensor);
	sensors.push( sensor );
    })
    .on('end',function(){
	//console.log('getSensorStatus query end');
	var msg = {
                topic: 'sensorStatus',
                payload: JSON.stringify(sensors)
                };

        sendMessage(msg);	
    });
}
