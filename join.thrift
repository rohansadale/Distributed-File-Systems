include "node.thrift"
service JoinService
{
	i64 Join(1:string ipaddress,2:i32 port),
	list<node.Node> PostJoin(1:node.Node node),
	node.Node GetNode()
}
