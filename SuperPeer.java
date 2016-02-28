import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;
import java.io.*;

/*
	SuperPeer Node.
	All nodes that participate in DHT must register themselves with SuperPeer
*/

public class SuperPeer
{
	public static int PORT 								= 9090; //Port Number where Super Peer will listen to requests
	private static String MOD_KEY						= "MOD";
	private static String CONFIG_FILE_NAME              = "";
	public static int MOD								= 32;  //Maximum number of Nodes in the network
	public static JoinService.Processor processor;
	public static void main(String[] targs) throws TTransportException
	{
		if(targs.length==1)
        {
            CONFIG_FILE_NAME            = targs[0];
            setParameters();
        }
		
		TServerTransport serverTransport    = new TServerSocket(PORT); //Opening Socket for listening to connection requests
        TTransportFactory factory 			= new TFramedTransport.Factory();
		JoinProtocolHandler join			= new JoinProtocolHandler(MOD);
		processor							= new JoinService.Processor(join);
		TServer.Args args                   = new TServer.Args(serverTransport);
        args.processor(processor);  //Set handler
		args.transportFactory(factory);  //Set FramedTransport (for performance)
		System.out.println("Starting Super Peer...");
        TServer server                      = new TSimpleServer(args); //Starting the server
        server.serve(); //Serve the requests
	}
	public static void setParameters()
    {
        String content;
        BufferedReader br       = null;
        try
        {
            br                  = new BufferedReader(new FileReader(CONFIG_FILE_NAME));
            while((content = br.readLine()) != null)
            {
				String[] tokens = content.split(":");
                if(tokens.length==2 && tokens[0].equals(MOD_KEY)==true)
                    MOD                 = Integer.parseInt(tokens[1]);
            }
        }
        catch(IOException e) {}
        finally
        {
            try
            {
                if(br!=null) br.close();
            }
            catch(IOException e) {}
        }
    }
}
