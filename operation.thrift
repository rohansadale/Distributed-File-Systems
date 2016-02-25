include "node.thrift"
include "path.thrift"
service OperationService
{
    path.Path Write(1:string filename,2:string content,3:bool shouldWrite,4:string basePath),
    path.Path read(1:string filename,2:bool shouldRead,3:string basePath),
    bool UpdateDHT(1:list<node.Node> node),
}
