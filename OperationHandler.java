import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;
import org.apache.thrift.TException;

import java.lang.Math;
import java.io.*;
import java.util.List;
import java.util.ArrayList;

public class OperationHandler implements OperationService.Iface
{
	/*
    Function that takes String as input generates a hash value between [0-31]
    */
	long getNodeId(String input)
    {
        long hash = 5381;
        for (int i = 0; i < input.length() ;i++)
        {
            hash = ((hash << 5) + hash) + input.charAt(i)*26;
            hash = hash%DHTNode.MOD;
        }
        return hash;
    }

	/*
	Function that takes file name and its content as input. File with filename is created and content is dumped in that file
	*/
	boolean WriteContentToFile(String filename,String content,String basePath) 
	{
		try
            {
                BufferedWriter br   = new BufferedWriter(new FileWriter(basePath + filename));
                br.write(content);
                br.close();
            }
            catch(IOException e)
            {
                return false;
            }
            return true;
	}

	/*
	Function to decided whether 
	(i) file should be written on this node
	(ii)file should be read from this node
	
	Parameters are:- hashedFileId(hash-value of filename),currentNodeId(NodeId of current Node)
	returns true/false whether processing could be done at this node or not.
	*/
	public boolean shouldProcessAtThisNode(long hashedFileId,long currentNodeId)
	{
		boolean shouldWrite		= false;
		Node previousNode       = DHTNode.previousNode;
        shouldWrite             = shouldWrite || (hashedFileId == currentNodeId); //check if hashedFileId equals currentNodeId
		shouldWrite				= shouldWrite || (currentNodeId == DHTNode.previousNode.id);
	
		//Checking if current node is successor of previous node and hashedfileId is between those two nodeId
        if(previousNode.id > currentNodeId)
        {
			//Handling round cases i.e nodes are at end and beginning of DHT cycle.
            if(hashedFileId <= currentNodeId) shouldWrite = true;
            if(hashedFileId > previousNode.id) shouldWrite = true;
        }
        if( previousNode.id < hashedFileId && currentNodeId >= hashedFileId) shouldWrite = true;
		
		return shouldWrite;
	}

	/*
	Function to determine the nodeid where requests should be forwarded next. In additiion to this, function also returns boolean denoting whether next node is 
	the required node i.e whether on next node read/write operation should be performed
	*/
	public Pair getNextNode(long currentNodeId,long hashedFileId)
	{
		long  start                     = currentNodeId;
        int i                           = 0;
        int nextNodeIdx                 = -1;
        boolean shouldWriteOnNextNode   = false;

        if(currentNodeId > DHTNode.fingerTable.get(0).id) //Corner-Case :- File's Hashed value is betwen Last-node and first node
        {
            if(hashedFileId > currentNodeId || hashedFileId <= DHTNode.fingerTable.get(0).id)
            {
                shouldWriteOnNextNode   = true;
                nextNodeIdx             = 0;
            }
            start = 0;
        }

        for(;i<DHTNode.fingerTable.size() && !shouldWriteOnNextNode;i++)
        {
            if(DHTNode.fingerTable.get(i).id < start) //Reached  end of the ring  
            {
                nextNodeIdx             = i-1;
                break;
            }
            if(DHTNode.fingerTable.get(i).id >= hashedFileId && start <= hashedFileId) //Found successor
            {
                nextNodeIdx             = i;
                shouldWriteOnNextNode   = true;
                break;
            }
            start                       = DHTNode.fingerTable.get(i).id;
        }

        if(nextNodeIdx == -1) //Corner-case:- When file's hash value is less than nodeid of every node present in finger table. In that case just go to last node
            nextNodeIdx                 = DHTNode.fingerTable.size()-1;
		
		return new Pair(nextNodeIdx,shouldWriteOnNextNode);
	}

	/*
	Function that actually established connection with next node where requests must be forwarded.
	Parameters:-
		* ip -> IP address of node where requests must be forwarded
		* port -> Port on which connection is to established
		* filename -> Name of file that is to created
		* content -> content that we are supposed to dump in that file
		* shouldWriteOnNextNode -> boolean flag telling whether we should perform actual task of writing on next node or not.
	Returns true/false whether write succeded or not
	*/	
	Path transferWriteRequest(String ip,int port,String filename,String content,boolean shouldWriteOnNextNode,String basePath) throws TException
	{
		TTransport OStransport          = new TSocket(ip,port);
        TProtocol OSprotocol            = new TBinaryProtocol(new TFramedTransport(OStransport));
        OperationService.Client OSclient= new OperationService.Client(OSprotocol);
        OStransport.open();
        Path nextStop 		          	= OSclient.Write(filename,content,shouldWriteOnNextNode,basePath);
        nextStop.route.add(DHTNode.currentNode.ip);
        nextStop.port.add(DHTNode.currentNode.port);
		OStransport.close();
        return nextStop;
	}
	
	/*
	Function that actually established connection with next node where requests must be forwarded.
	Parameters:-
		* ip -> IP address of node where requests must be forwarded
		* port -> Port on which connection is to established
		* filename -> Name of file from which content is to read
		* shouldWriteOnNextNode -> boolean flag telling whether we should perform actual task of reading on next node or not.
	Returns String having file Contents
	*/	
	Path transferReadRequest(String ip,int port,String filename,boolean shouldReadFromNextNode,String basePath) throws TException
	{
		TTransport OStransport          = new TSocket(ip,port);
        TProtocol OSprotocol            = new TBinaryProtocol(new TFramedTransport(OStransport));
        OperationService.Client OSclient= new OperationService.Client(OSprotocol);
        OStransport.open();
        Path nextStop 		            = OSclient.read(filename,shouldReadFromNextNode,basePath);
        nextStop.route.add(DHTNode.currentNode.ip);
        nextStop.port.add(DHTNode.currentNode.port);
        OStransport.close();
        return nextStop;
	}

	/*
	Function that reads from file filename and returns the content as String
	*/
	public String readFromFile(String filename,String basePath)
	{
		String content;
        BufferedReader br       = null;
        StringBuilder sb        = new StringBuilder();
		if(new File(basePath+filename).exists() == false) return "NIL";
        try
        {
            br                  = new BufferedReader(new FileReader(basePath+filename));
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
	
	/*
	Interface method implemented
	Parameters:-
		* filename -> Name of file that is to created
		* content -> content that we are supposed to dump in that file
		* shouldWrite -> boolean denoting whether we should write on this node
	This function checks whether we need to write data on this node. If no,then it recursively passes requests to next node
	Returns true/false whether write succeded or not
	*/
	@Override
	public Path Write(String filename,String content,boolean shouldWrite,String basePath) throws TException
	{
		long hashedFileId		= getNodeId(filename);
		long currentNodeId		= DHTNode.currentNode.id;
		shouldWrite				= shouldWrite  || shouldProcessAtThisNode(hashedFileId,currentNodeId);
	
		if(shouldWrite)
		{
			System.out.println("Writing file " + filename + " with hash value " + hashedFileId + " on node " + DHTNode.currentNode.ip);
			boolean hasWritten		= WriteContentToFile(filename,content,basePath);
			ArrayList<String> route = new ArrayList<String>();
			ArrayList<Integer> port = new ArrayList<Integer>();
			route.add(DHTNode.currentNode.ip);
			port.add(DHTNode.currentNode.port);
			Path result				= new Path("",hasWritten,route,port);
			return result;
		}
	
		Pair P 					= getNextNode(currentNodeId,hashedFileId);	
		return transferWriteRequest(DHTNode.fingerTable.get(P.getFirst()).ip,DHTNode.fingerTable.get(P.getFirst()).port,filename,content,P.getSecond(),basePath);
	}

	/*
	Interface method implemented
	Parameters:-
		* filename -> Name of file from which we should read
		* shouldRead -> boolean denoting whether we should read from this node
	This function checks whether we need to read data from this node. If no,then it recursively passes requests to next node
	Returns String having file contents
	*/
	@Override
	public Path read(String filename,boolean shouldRead,String basePath) throws TException
	{
		long hashedFileId       = getNodeId(filename);
        long currentNodeId      = DHTNode.currentNode.id;
        shouldRead              = shouldRead  || shouldProcessAtThisNode(hashedFileId,currentNodeId);
        
		if(shouldRead)
        {
            System.out.println("Reading file " + filename + " with hash value " + hashedFileId + " from node " + DHTNode.currentNode.ip);
            String content			= readFromFile(filename,basePath);
			ArrayList<String> route = new ArrayList<String>();
			ArrayList<Integer> port = new ArrayList<Integer>();
			route.add(DHTNode.currentNode.ip);
			port.add(DHTNode.currentNode.port);
			Path result				= new Path(content,false,route,port);
			return result;
		}

        Pair P                  = getNextNode(currentNodeId,hashedFileId);
        return transferReadRequest(DHTNode.fingerTable.get(P.getFirst()).ip,DHTNode.fingerTable.get(P.getFirst()).port,filename,P.getSecond(),basePath);
	}

	/*
	Interface method implemented
	This is function that gets called when a new node join the network.
	Current node checks whether it needs to update its finger table or not.
	If yes, then it updates its finger table and along with its previous node.
	*/
	@Override
	public boolean UpdateDHT(List<Node> latestNode)
	{
		long currentNodeId 	= DHTNode.currentNode.id;
		DHTNode.fingerTable.clear();
		for(int i=1;i<=DHTNode.fingerTableSize;i++)
		{
			int succ = (int)(currentNodeId + (1 << (i-1)))%DHTNode.MOD;
            int j    = 0;
            for(j=0;j<latestNode.size();j++)
            {
                if(latestNode.get(j).id > succ) break;
            }
            if(j == latestNode.size()) DHTNode.fingerTable.add(latestNode.get(0));
            else DHTNode.fingerTable.add(latestNode.get(j));
		}
        
		DHTNode.previousNode	= null;
		for(int j=0;j<latestNode.size();j++)
		{
			if(latestNode.get(j).id < DHTNode.currentNode.id)
				DHTNode.previousNode	= latestNode.get(j);
			else break;
		}
		if(null==DHTNode.previousNode) DHTNode.previousNode = latestNode.get(latestNode.size()-1);
		DHTNode.printFingerTable();	
		return true;
	}
}
