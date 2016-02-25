include "node.thrift"
service JoinService
{
	i64 Join(1:string ipaddress,2:i32 port),
	list<node.Node> addToDHT(1:node.Node node),
	void PostJoin(),
	node.Node GetNode()
}
