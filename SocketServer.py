import socket
from General import General
from Clock import Clock

class SocketServer (General):
	def __init__(self, host = "localhost", port = 8080, _buffer = 1024, maxConnections = 10, displayMsg = False, allowMultipleConnections = True):
		self.host = host;
		self.port = port;
		self._buffer = _buffer;
		self.maxConnections = maxConnections;
		self.waiting = False
		self.shouldWait = True
		self.closing = False;
		self.closed = False;
		self.connections = []
		self.allowMultipleConnections = allowMultipleConnections
		self.displayMessageCallback = displayMsg or self.displayMsg
		# Callback definitions
		self.onStartCallback = self.emptyFunction;
		self.onConnectCallback = self.emptyFunction;
		self.onReceiveCallback = self.emptyFunction;
		self.onSendCallback = self.emptyFunction;
		self.onDisconnectCallback = self.emptyFunction;


	def displayMsg (self, code = 0):
		print(code);


	def emptyFunction (self, *args, **kwargs):
		pass;


	def start (self):
		try:
			self.server = socket.socket();
			self.server.bind((self.host, self.port));
			self.server.listen(self.maxConnections);
			self.Clock = Clock();

			self.onStartCallback(True, None);
		except Exception as e:
			self.onStartCallback(False, e);


	def waitForConnections (self, _async = False):
		def method (**kwargs):
			kwargs["self"].waitForConnection(kwargs["_async"]);

		self.setInterval(method, 1, {"self": self, "_async": _async});


	def waitForConnection(self, _async = False):
		if self.shouldWait and not self.waiting:
			self.waiting = True;

			if _async:
				self.setImmediate(self.onBeforeConnectCallback);
			else:
				self.onBeforeConnectCallback();				


	def onBeforeConnectCallback(self):
		try:
			(conn, addr) = self.server.accept();

			if not self.shouldWait:
				return False;

			if not self.allowMultipleConnections:
				for client in self.connections:
					pass
			client = {
				"index": len(self.connections),
				"conn": conn,
				"ip": addr[0],
				"port": addr[1],
				"addr": addr,
				"isBlocked": False,
				"isReceiving": False,
				"isSending": False,
				"isConnected": True,
				"date": self.Clock.date(),
				"time": self.Clock.time()
			}

			self.connections.append(client);

			self.onConnectCallback(client, None); # Trigger callback
		except Exception as e:
			self.onConnectCallback(False, e); # Trigger callback


		self.waiting = False;


	def recv (self, client, _async = False):
		self.receive(client, _async);


	def receive (self, client, _async = False):
		if self.isClient(client):

			client = self.refreshClientData(client);

			if not client["isSending"]:
				self.setClientData(client, {"isSending": True});

				client = self.refreshClientData(client);

				if _async:
					self.setImmediate(self.onBeforeReceiveCallback, {"client": client});
				else:
					self.onBeforeReceiveCallback(client = client);


	def startRecvFrom (self, client, _async = False):
		self.startReceivingFrom(client, _async);


	def startReceivingFrom (self, client, _async = False):
		if _async:
			def method (**kwargs):
				if self.isClient(client):
					self.receive(client);

			self.setInterval(method, 1, {"self": self});
		else:
			while True:
				if self.isClient(client):
					self.receive(client);


	def onBeforeReceiveCallback(self, **kwargs):
		try:
			data = kwargs["client"]["conn"].recv(self._buffer)
			data = data.decode("utf-8")
			if data != "":
				data = self.jsonize(data); # Convert the data (in string format) to dict

				self.onReceiveCallback(data, kwargs["client"], None); # Trigger callback
		except Exception as e:
			self.onReceiveCallback(False, kwargs["client"], e); # Trigger callback


		self.setClientData(kwargs["client"], {"isSending": False});


	def send (self, client, data, _async = False):
		if self.isClient(client):

			client = self.refreshClientData(client);

			if not client["isReceiving"]:

				self.setClientData(client, {"isReceiving": True});

				if _async:
					self.setImmediate(self.onBeforeSendCallback, {"data": data, "client": client});
				else:
					self.onBeforeSendCallback(data = data, client = client);


	def onBeforeSendCallback (self, **kwargs):
		try:
			data = {
				"date": self.Clock.date(),
				"time": self.Clock.time(),
				"data": kwargs["data"]
			}

			kwargs["client"]["conn"].send(self.unjsonize(data).encode("utf-8"));

			self.onSendCallback(data, kwargs["client"], None);
		except Exception as e:
			self.onSendCallback(False, kwargs["client"], e);


		self.setClientData(kwargs["client"], {"isReceiving": False});


	def isClient(self, client):
		if client["index"] >= len(self.connections):
			return False;

		if client["ip"] != self.connections[client["index"]]["ip"]:
			return False;

		if (not self.allowMultipleConnections
		    and client["port"] != self.connections[client["index"]]["port"]):
			return False

		if not (self.connections[client["index"]]["isConnected"]) or self.connections[client["index"]]["isBlocked"]:
			return False

		return True;


	def refreshClientData (self, client):
		if self.isClient(client):
			return self.connections[client["index"]];


	def setClientData (self, client, data):
		if self.isClient(client):
			self.connections[client["index"]] = {**self.connections[client["index"]], **data};


	def block (self, client):
		if self.isClient(client):
			self.connections[client["index"]]["isBlocked"] = True;


	def unBlock (self, client):
		if self.isClient(client):
			self.connections[client["index"]]["isBlocked"] = False;


	def disconnectAll (self, _async = False):
		if _async:
			self.setImmediate(self._disconnectAll);
		else:
			self._disconnectAll();


	def _disconnectAll (self):
		for client in self.connections:
			self.disconnect(client);


	def disconnect (self, client, _async = False):
		if _async:
			self.setImmediate(self.onBeforeDisconnectCallback, {"client": client});
		else:
			self.onBeforeDisconnectCallback(client = client);


	def onBeforeDisconnectCallback (self, **kwargs):
		if self.isClient(client):
			try:
				client = self.refreshClientData(kwargs["client"]);

				client["conn"].close(); # Close the client's connection

				self.setClientData(client, {"isConnected": False}); # Change the client's status to "disconnected"

				self.onDisconnectCallback(client, None); # Trigger the callback
			except Exception as e:
				self.onDisconnectCallback(False, e);


	def onStart (self, callback):
		self.onStartCallback = callback;


	def onConnect (self, callback):
		self.onConnectCallback = callback;


	def onReceive (self, callback):
		self.onReceiveCallback = callback


	def onRecv (self, callback):
		self.onReceiveCallback = callback


	def onSend (self, callback):
		self.onSendCallback = callback


	def onDisconnect (self, callback):
		self.onDisconnectCallback = callback;


	def close (self):
		self.shouldWait = False;
		self.closing = True;

		for client in self.connections:
			if client["isConnected"]:
				self.send(client, "Disconnecting you...");
				self.disconnect(client);

		self.server.close();
		self.closed = True;

if __name__ == "__main__":
	server = SocketServer();

	def onConnect(client, err):
		if err:
			print("An error occurred while a client was trying to connect");
			print(err);
		else:
			print(f'Received a new connection from {client["ip"]}');
			server.send(client, "Welcome!");
			server.startReceivingFrom(client);

	def onStart (started, err):
		if started:
			print("The server has started running!");

	def onRecv(data, client, err):
		if err:
			print(f'An error occurred while receiving data from {client["ip"]}');
			print(err);
		else:
			print(f'Data has been received from {client["ip"]}');
			print(data);

	server.onStart(onStart);
	server.onConnect(onConnect);
	server.onReceive(onRecv);

	server.start();
	server.waitForConnection();