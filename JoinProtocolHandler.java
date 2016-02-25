import org.apache.thrift.TException;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.lang.System;

public class JoinProtocolHandler implements JoinService.Iface
{
	private List<Node> activeNodes;
	private boolean isBusy; //Boolean denoting whether Super Peer is busy i.e other node is in process of joining DHT
	private int seed = 42;	
	private int MOD  = 32;

	/*
	Constructor to initialize class variables
	*/
	public JoinProtocolHandler()
	{
		activeNodes	 = new ArrayList<Node>(); //Maintaing active nodes in DHT
		isBusy       = false;
	}

	/*
	Function that takes String as input generates a hash value between [0-31]
	*/
	long getNodeId(String input)
	{
		long hash = 5381;
		for (int i = 0; i < input.length() ;i++) 
		{
			hash = ((hash << 5) + hash) + input.charAt(i)*26;
			hash = hash%MOD;		
		}
		return hash;
	}

	/*
	Interface function extended
	Function takes (ipaddress,port) and adds that node to DHT
	*/
	@Override
	public long Join(String ipaddress,int port) throws TException
	{
		System.out.println("Requesting for connection .....");
		if(isBusy) return -1; //If Super Peer is busy i.e other node is also joining DHT then this node will have to wait till other node have joined the process
		long nodeid = getNodeId(ipaddress+port);
		isBusy = true;
		return nodeid;
	}
	
	/*
	Interface function extended
	This function updates the list where active nodes are maintained. In addittion to this, it also signals that other nodes that want to join DHT 
	can go ahead with joining process
	*/
	@Override
	public List<Node> addToDHT(Node node) throws TException
	{
		System.out.println("Acknowleding after joining DHT ....");
		boolean isrecorded 	= activeNodes.add(node); //Keeping the nodes sorted order with respect to node-id
		Collections.sort(activeNodes,new Comparator<Node>()
		{
			@Override
			public int compare(Node lhs,Node rhs) 
			{
				if(lhs.id < rhs.id) return -1;
				else return 1;
			}
		});

		System.out.println("Currently connected Node in DHT ... ");
		System.out.println("---------------------------------------------------------");
		System.out.println("        HostName               Port      NodeId          ");
		System.out.println("---------------------------------------------------------");
		for(int i=0;i<activeNodes.size();i++)	
		{
			System.out.println(activeNodes.get(i).ip + "    " + activeNodes.get(i).port + "        " + activeNodes.get(i).id);
			System.out.println("---------------------------------------------------------");
		}
		
		System.out.println("\n\n");	
		if(!isrecorded) return null;
		return activeNodes;
	}

	/*
	Interface method Implemented
	This method will basically update status of SuperPeer to available i.e SuperPeer is now ready to allow other nodes to join the network
	*/
	@Override
	public void PostJoin()
	{
		isBusy	= false;
	}
	
	/*
	Interface implemented extended
	Returns a random from where process of read or write begins
	*/	
	@Override
	public Node GetNode() throws TException
	{
		System.out.println("Selecting random Node from DHT ....");
		int seed = (int)((long)System.currentTimeMillis() % 1000);
		Random rnd = new Random(seed);				
		return activeNodes.get(rnd.nextInt(activeNodes.size()));
	} 
}
