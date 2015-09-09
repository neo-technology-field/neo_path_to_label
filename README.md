# neo_path_to_label
Example Traversal API finding the first shortest path to a node with a specific label

# Instructions

1. Build it:

        mvn clean package

2. Copy target/path_to_label-1.0.jar to the plugins/ directory of your Neo4j server.


3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=com.maxdemarzi=/v1

        
4. Start Neo4j server.

5. Check that it is installed correctly over HTTP:

        :GET /v1/service/helloworld
        
6. Try the extension:
        
        :GET /v1/service/path_to/{Label}/from/{node_id}"
        :GET /v1/service/path_to/{Label}/from/{node_id}?direction=incoming"
        :GET /v1/service/path_to/{Label}/from/{node_id}?direction=outgoing"
        :GET /v1/service/path_to/{Label}/from/{node_id}?depth=5"
        :GET /v1/service/path_to/{Label}/from/{node_id}?direction=outgoing&depth=5"
        
7. Additional Queries: 

Example label is "vdp". 

Query 1:
        
        MATCH (donor)
        WHERE id(donor) = 215
        MATCH (donor)-[ds_rel]->(d_sample:`vdp`:`vdp|VRTrack|Sample`)
        RETURN donor,ds_rel,d_sample

        :GET /v1/service/query1/{Label}/{node_id}
        
Query 2:

        MATCH (donor)-[:sample]->(sample)
        WHERE id(donor) = 215
        MATCH (donor)<-[:member]-(study:`vdp`:`vdp|VRTrack|Study`)<-[:has]-(group:`vdp`:`vdp|VRTrack|Group`)
        OPTIONAL MATCH (group)<-[ar:administers]-(auser:`vdp`:`vdp|VRTrack|User`)
        OPTIONAL MATCH (sample)-[fbr:failed_by]->(fuser:`vdp`:`vdp|VRTrack|User`)
        OPTIONAL MATCH (sample)-[sbr:selected_by]->(suser:`vdp`:`vdp|VRTrack|User`)
        OPTIONAL MATCH (sample)-[pbr:passed_by]->(puser:`vdp`:`vdp|VRTrack|User`)
        RETURN donor,sample,group,ar,auser,fbr,fuser,sbr,suser,pbr,puser
        
        :GET /v1/service/query2/{Label}/{node_id}
        
        