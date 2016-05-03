import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.TThreadPoolServer;

import java.lang.Thread;
import java.net.InetAddress;
import java.net.UnknownHostException; 
import java.util.List;
import java.util.ArrayList;
import java.io.*;

public class DHTNode
{
	private static String SUPER_PEER_IP  				= ""; //Remote Address of Super Peer
	private static int PORT								= 0; //Port Number of Super Peer where it will listen to requests
	private static long SLEEP_TIMEOUT					= 5000; //Sleep time  i.e Time to wait before againt asking super peer for connection
	private static String CURRENT_NODE_IP				= "";   //Current Node IP
	private static int CURRENT_NODE_PORT 				= 9091;	//Port Number where Current Node will listen to requests
	private static boolean isRegistered					= false;
	private static OperationService.Processor processor;
	private static long nodeid							= -1;
	private static String SUPER_PEER_KEY			 	= "SuperPeer";
	private static String PORT_KEY						= "Port"; 	
	private static String FINGER_TABLE_SZ_KEY		 	= "FingerTableSize";
	private static String MOD_KEY						= "MOD"; 	

	private static String CONFIG_FILE_NAME              = "";
	public static int MOD								= 0;  //Maximum of 32 Nodes in the network
	public static List<Node> activeNodes				= null; //List of nodes currently in DHT
	public static int fingerTableSize					= 0; //Maximum number of entries in DHT
	public static Node currentNode						= null; 
	public static ArrayList<Node> fingerTable			= null; //finger table for this node
	public static Node previousNode						= null; //Pointer of previous node

    public static void main(String []targs) throws TException
    {
		//Setting IPAddress of current node
		try
		{
			CURRENT_NODE_IP			= InetAddress.getLocalHost().getHostName(); 
		}
		catch(Exception e)
		{
			System.out.println("Unable to get hostname ....");
		}

		if(CURRENT_NODE_IP=="")
		{
			System.out.println("Unable to get Current System's IP");
			return;
		}
	
		if(targs.length >=1 )
        {
            CONFIG_FILE_NAME            = targs[0];
            setParameters();
        }
		
		if(targs.length >=2)
		{
			CURRENT_NODE_PORT			= Integer.parseInt(targs[1]);
		}
	
        if("".equals(SUPER_PEER_IP)==true)
        {
            System.out.println("Unable to connect to SuperPeer: IP Address Missing");
            return;
        }
        if(0==PORT)
        {
            System.out.println("Unable to connect to SuperPeer: Port Missing");
            return;
        }

		try
		{	
        	TTransport transport 		= new TSocket(SUPER_PEER_IP,PORT);  //Establishing connection with Super Peer
			TProtocol protocol   		= new TBinaryProtocol(new TFramedTransport(transport));
        	JoinService.Client client 	= new JoinService.Client(protocol);
        	transport.open();
		
			while(true)
			{
				System.out.println("Waiting for connection ......");
				nodeid					= client.Join(CURRENT_NODE_IP,CURRENT_NODE_PORT);
				if(nodeid >= 0)
				{
					currentNode			= new Node(CURRENT_NODE_IP,CURRENT_NODE_PORT,nodeid); //Successfull established the connection
					activeNodes			= client.addToDHT(currentNode);
					if(activeNodes!=null) 
						isRegistered = true;
					else 
						System.out.println("Unable to add node to DHT !!!!!");
					break;
				}
				else if(nodeid==-2)
				{	
					System.out.println("DHT has reached its maximum capacity and hence no new node can join the network");
					break;
				}
				try
				{
					Thread.sleep(SLEEP_TIMEOUT); //Waiting for SLEEP_TIMEOUT seconds before again trying for connection
				}
				catch(InterruptedException e) 
				{
						System.out.println("In Exception ");
				}
			}

			if(isRegistered)
			{
					updateFingerTable(); //Updating the finger table
					printFingerTable(); //Utility function to print finger table
					contactNodes(); //Contact other nodes in the network so that they could update their finger Table
					client.PostJoin(); //Notifying Super-Peer that current node's information has been shared with all other nodes in P2P system
					transport.close();
					System.out.println("Starting thrift server at host " + CURRENT_NODE_IP);
					TServerTransport serverTransport    = new TServerSocket(CURRENT_NODE_PORT); //Starting thrift server so that other nodes could contact
					TTransportFactory factory           = new TFramedTransport.Factory();
					OperationHandler operate            = new OperationHandler();
					processor                           = new OperationService.Processor(operate);
					TThreadPoolServer.Args args         = new TThreadPoolServer.Args(serverTransport);
					args.processor(processor);  //Set handler
					args.transportFactory(factory);  //Set FramedTransport (for performance)
					System.out.println("Starting the simple server...");
					TThreadPoolServer server            = new TThreadPoolServer(args);
					server.serve(); //Serving the requests
			}
		}
		catch(TException x)
		{
				System.out.println(" =================== Unable to establish connection with SuperPeer ... Exiting ... =================");
				return;
		}
	}

	/*
	   Function to update finger table. Each entry in finger table is filled with succ(nodeid + 2^(i-1)) where 1<=i<=5
	   In addition to this, function also stores previous node for given node.
	 */
	private static void updateFingerTable()
	{
			fingerTable = new ArrayList<Node>();
			for(int i=1;i<=fingerTableSize;i++) 
			{
					int succ = (int)(nodeid + (1 << (i-1)))%MOD;		
					int j    = 0;	
					for(j=0;j<activeNodes.size();j++)
					{
							if(activeNodes.get(j).id > succ) break;
					}
					if(j == activeNodes.size()) fingerTable.add(activeNodes.get(0)); 
					else fingerTable.add(activeNodes.get(j));
			}

			//Updating previous Node	
			for(int i=0;i<activeNodes.size();i++)
			{
					if(activeNodes.get(i).id < currentNode.id) previousNode    = activeNodes.get(i);
					else break;
			}
			if(null == previousNode) previousNode = activeNodes.get(activeNodes.size()-1);
	}

	/*
	   Utility function to print Finger table
	 */
	public static void printFingerTable()
	{
			System.out.println("Finger Table for Node with HostName :- " + CURRENT_NODE_IP);
			System.out.println("---------------------------------------------------------");
			System.out.println("        HostName               Port      NodeId          ");
			System.out.println("---------------------------------------------------------");
			for(int i=0;i<fingerTable.size();i++)
			{
					System.out.println(fingerTable.get(i).ip + "    " + fingerTable.get(i).port + "        " + fingerTable.get(i).id);
					System.out.println("---------------------------------------------------------");
			}
			System.out.println("Previous Node:-   " + previousNode.ip + "   "  + previousNode.port + "   " + previousNode.id);
			System.out.println("\n\n");
	}

	/*
	   In this function connection with other nodes is established so that other nodes could know about existence of this node and correspondingly update finger table
	 */
	private static void contactNodes() throws TException
	{
			for(int i=0;i<activeNodes.size();i++)
			{
					if(CURRENT_NODE_IP.equals(activeNodes.get(i).ip) == true) continue;
					System.out.println("Contacting node with IP " + activeNodes.get(i).ip + " and nodeId " + activeNodes.get(i).id);
					TTransport transport            = new TSocket(activeNodes.get(i).ip,activeNodes.get(i).port); //Establishing connection with other nodes in P2P network
					TProtocol protocol              = new TBinaryProtocol(new TFramedTransport(transport));
					OperationService.Client client  = new OperationService.Client(protocol);
					transport.open();
					client.UpdateDHT(activeNodes); //UpdateDHT which updates the finger table of other nodes in DHT
					transport.close();
					System.out.println("Updated finger table for node with IP " + activeNodes.get(i).ip + " and nodeId " + activeNodes.get(i).id + "\n\n");
			}
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
							if(tokens.length==2 && tokens[0].equals(SUPER_PEER_KEY)==true)
									SUPER_PEER_IP       = tokens[1];
							if(tokens.length==2 && tokens[0].equals(PORT_KEY)==true)
									PORT                = Integer.parseInt(tokens[1]);
							if(tokens.length==2 && tokens[0].equals(FINGER_TABLE_SZ_KEY)==true)
									fingerTableSize     = Integer.parseInt(tokens[1]);
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
