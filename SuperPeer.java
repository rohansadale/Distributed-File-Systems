import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;

/*
	SuperPeer Node.
	All nodes that participate in DHT must register themselves with SuperPeer
*/

public class SuperPeer
{
	public static int PORT = 9090; //Port Number where Super Peer will listen to requests
	public static JoinService.Processor processor;
	public static void main(String[] targs) throws TTransportException
	{
		TServerTransport serverTransport    = new TServerSocket(PORT); //Opening Socket for listening to connection requests
        TTransportFactory factory 			= new TFramedTransport.Factory();
		JoinProtocolHandler join			= new JoinProtocolHandler();
		processor							= new JoinService.Processor(join);
		TServer.Args args                   = new TServer.Args(serverTransport);
        args.processor(processor);  //Set handler
		args.transportFactory(factory);  //Set FramedTransport (for performance)
		System.out.println("Starting Super Peer...");
        TServer server                      = new TSimpleServer(args); //Starting the server
        server.serve(); //Serve the requests
	}
}
