import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;
import java.io.*;
import java.lang.Thread;

/*
   Helper file that establishes connection with super peer to get the node from which one should start the process.
   After getting node id, this client established the connection with given node and passes the required parameter to accomplish the task
 */

public class Worker
{
		private static String SUPER_PEER_KEY				= "SuperPeer";
		private static String PORT_KEY						= "Port";
		private static String READ_DIR_KEY                  = "ReadDirectory";
		private static String WRITE_DIR_KEY					= "WriteDirectory";
		private static String VERBOSE_KEY					= "Verbose";
		private static String CONFIG_FILE_NAME				= "";

		private static String SUPER_PEER_IP                 = "";
		private static int PORT                             = 0;
		private static String READ_DIR                      = "./";
		private static String WRITE_DIR 					= "./";
		private static int VERBOSE 							= 0;
		private static int Op                               = 0;// 0 => Read ; 1 => Write
		private static String filename                      = "";

		public static void main(String[] targs) throws TException
		{
				if(targs.length>=1)
				{
						CONFIG_FILE_NAME			= targs[0];
						setParameters();
				}

				if(targs.length >=2)
				{
						Op  = Integer.parseInt(targs[1]);
				}

				if(targs.length >= 3)
				{
						filename    = targs[2];
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
				if("".equals(filename) == true)
				{
						System.out.println("Filename missing. Please provide name of the file that is to processed as command line parameter");
						return;
				}

				try
				{
						TTransport transport    	    = new TSocket(SUPER_PEER_IP,PORT);
						TProtocol protocol          	= new TBinaryProtocol(new TFramedTransport(transport));
						JoinService.Client client   	= new JoinService.Client(protocol);
						transport.open();
						Node startNode	 				= client.GetNode();
						transport.close();

						System.out.println("Initial Connection to " + startNode.ip + " on port " + startNode.port);
						TTransport OStransport			= new TSocket(startNode.ip,startNode.port);
						TProtocol OSprotocol			= new TBinaryProtocol(new TFramedTransport(OStransport));
						OperationService.Client OSclient= new OperationService.Client(OSprotocol);
						OStransport.open();
						Path result                     = null;
						if(0==Op) 
						{
								System.out.println("Reading file " + filename + " FROM DHT");
								result 					= OSclient.read(filename,false,WRITE_DIR);
								if(result.content.equals("NIL")==true)
										System.out.println("File not present in DHT !!!");
								System.out.println("File Content :- " + result.content);
						}
						else 
						{
								System.out.println("Writing file " + filename + " FROM DHT");
								result                  = OSclient.Write(filename,getFileContent(READ_DIR+filename),false,WRITE_DIR);
						}
						for(int i=result.route.size()-1,j=1;i>=0 && VERBOSE!=0;i--,j++)
						{
								System.out.print(j+". "+ result.route.get(i)+":"+result.port.get(i));
								if(0==i) System.out.print(" [File Read from this Node]");
								System.out.println("");
						}
						System.out.println("\n\n");
						OStransport.close();
				}
				catch(TException e)
				{
						System.out.println(" =================== Unable to establish connection with SuperPeer ... Exiting ... =================");
						return;
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
										SUPER_PEER_IP 		= tokens[1];
								if(tokens.length==2 && tokens[0].equals(PORT_KEY)==true)
										PORT 				= Integer.parseInt(tokens[1]);
								if(tokens.length==2 && tokens[0].equals(READ_DIR_KEY)==true)
										READ_DIR            = tokens[1];
								if(tokens.length==2 && tokens[0].equals(WRITE_DIR_KEY)==true)
										WRITE_DIR 	 		= tokens[1];
								if(tokens.length==2 && tokens[0].equals(VERBOSE_KEY)==true)
										VERBOSE 	 		= Integer.parseInt(tokens[1]);
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

		public static String getFileContent(String filename)
		{
				String content;
				BufferedReader br       = null;
				StringBuilder sb        = new StringBuilder();
				try
				{
						br                  = new BufferedReader(new FileReader(filename));
						while((content = br.readLine()) != null)
								sb.append(content);
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
				return sb.toString();
		}

}
