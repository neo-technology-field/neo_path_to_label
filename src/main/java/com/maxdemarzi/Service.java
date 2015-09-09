package com.maxdemarzi;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Path("/service")
public class Service {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @GET
    @Path("/helloworld")
    public Response helloWorld() throws IOException {
        Map<String, String> results = new HashMap<String,String>(){{
            put("hello","world");
        }};
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    /*
    query1:
    MATCH (donor)
    WHERE id(donor) = 215
    MATCH (donor)-[ds_rel]->(d_sample:`vdp`:`vdp|VRTrack|Sample`)
    RETURN donor,ds_rel,d_sample
     */

    @GET
    @Path("/query1/{label}/{id}")
    public Response query1(@PathParam("label") String label,
                           @PathParam("id") Long id,
                           @Context GraphDatabaseService db) throws IOException {
        ArrayList results = new ArrayList();
        try (Transaction tx = db.beginTx()) {
            Node donor = db.getNodeById(id);
            HashMap<String, Object> donorProperties = getProperties(donor);


            for ( Relationship ds_rel : donor.getRelationships(Direction.OUTGOING, RelationshipTypes.ds_rel) ) {
                Node d_sample = ds_rel.getEndNode();
                if (d_sample.hasLabel(DynamicLabel.label(label + "|VRTrack|Sample"))) {

                    HashMap<String, Object> ds_relProperties = getProperties(ds_rel);
                    HashMap<String, Object> d_sampleProperties = getProperties(d_sample);
                    HashMap<String, Object> result = new HashMap<>();
                    result.put("donor", donorProperties);
                    result.put("ds_rel", ds_relProperties);
                    result.put("d_sample", d_sampleProperties);
                    results.add(result);
                }
            }
        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    /*
    query2:
    MATCH (donor)-[:sample]->(sample)
    WHERE id(donor) = 215
    MATCH (donor)<-[:member]-(study:`vdp`:`vdp|VRTrack|Study`)<-[:has]-(group:`vdp`:`vdp|VRTrack|Group`)
    OPTIONAL MATCH (group)<-[ar:administers]-(auser:`vdp`:`vdp|VRTrack|User`)
    OPTIONAL MATCH (sample)-[fbr:failed_by]->(fuser:`vdp`:`vdp|VRTrack|User`)
    OPTIONAL MATCH (sample)-[sbr:selected_by]->(suser:`vdp`:`vdp|VRTrack|User`)
    OPTIONAL MATCH (sample)-[pbr:passed_by]->(puser:`vdp`:`vdp|VRTrack|User`)
    RETURN donor,sample,group,ar,auser,fbr,fuser,sbr,suser,pbr,puser
     */

    @GET
    @Path("/query2/{label}/{id}")
    public Response query2(@PathParam("label") String label,
                           @PathParam("id") Long id,
                           @Context GraphDatabaseService db) throws IOException {
        ArrayList results = new ArrayList();
        try (Transaction tx = db.beginTx()) {
            Node donor = db.getNodeById(id);
            HashMap<String, Object> donorProperties = getProperties(donor);

            for (Relationship rel : donor.getRelationships(Direction.OUTGOING, RelationshipTypes.sample)) {
                Node sample = rel.getEndNode();
                HashMap<String, Object> sampleProperties = getProperties(sample);

                for (Relationship rel2 : donor.getRelationships(Direction.INCOMING, RelationshipTypes.member)) {
                    Node study = rel2.getStartNode();
                    if (study.hasLabel(DynamicLabel.label(label + "|VRTrack|Study"))) {
                        for (Relationship rel3 : study.getRelationships(Direction.INCOMING, RelationshipTypes.has)) {
                            Node group = rel3.getStartNode();
                            HashMap<String, Object> groupProperties = getProperties(group);
                            if (group.hasLabel(DynamicLabel.label(label + "|VRTrack|Group"))) {
                                HashMap<String, Object> result = new HashMap<>();

                                for ( Relationship ar : group.getRelationships(Direction.INCOMING, RelationshipTypes.administers) ){
                                    Node auser = ar.getStartNode();
                                    if (auser.hasLabel(DynamicLabel.label(label + "|VRTrack|User"))) {
                                        result.put("ar", getProperties(ar));
                                        result.put("auser", getProperties(auser));
                                    }
                                }

                                for ( Relationship fbr : sample.getRelationships(Direction.OUTGOING, RelationshipTypes.failed_by) ){
                                    Node fuser = fbr.getEndNode();
                                    if (fuser.hasLabel(DynamicLabel.label(label + "|VRTrack|User"))) {
                                        result.put("fbr", getProperties(fbr));
                                        result.put("fuser", getProperties(fuser));
                                    }
                                }

                                for ( Relationship sbr : sample.getRelationships(Direction.OUTGOING, RelationshipTypes.selected_by) ){
                                    Node suser = sbr.getEndNode();
                                    if (suser.hasLabel(DynamicLabel.label(label + "|VRTrack|User"))) {
                                        result.put("sbr", getProperties(sbr));
                                        result.put("suser", getProperties(suser));
                                    }
                                }

                                for ( Relationship pbr : sample.getRelationships(Direction.OUTGOING, RelationshipTypes.passed_by) ){
                                    Node puser = pbr.getEndNode();
                                    if (puser.hasLabel(DynamicLabel.label(label + "|VRTrack|User"))) {
                                        result.put("pbr", getProperties(pbr));
                                        result.put("puser", getProperties(puser));
                                    }
                                }

                                result.put("group", groupProperties);
                                result.put("sample", sampleProperties);
                                result.put("donor", donorProperties);
                                results.add(result);
                            }
                        }
                    }

                }

            }
        }
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    private HashMap<String, Object> getProperties(PropertyContainer container) {
        HashMap<String, Object> properties = new HashMap<>();
        for (String key : container.getPropertyKeys()) {
            properties.put(key, container.getProperty(key));
        }
        return properties;
    }

    @GET
    @Path("/path_to/{label}/from/{id}")
    public Response pathToLabel(@PathParam("label") String label,
                                @PathParam("id") Long id,
                                @DefaultValue("both") @QueryParam("direction") String dir,
                                @DefaultValue("20") @QueryParam("depth") Integer depth,
                                @Context GraphDatabaseService db) throws IOException {
        HashMap<String, Object> results = new HashMap<>();
        Direction direction;
        if (dir.toLowerCase().equals("incoming")) {
            direction = Direction.INCOMING;
        } else if (dir.toLowerCase().equals("outgoing")) {
            direction = Direction.OUTGOING;
        } else {
            direction = Direction.BOTH;
        }

        LabelEvaluator labelEvaluator = new LabelEvaluator(DynamicLabel.label(label));
        PathExpander pathExpander = PathExpanderBuilder.allTypes(direction).build();

        TraversalDescription td = db.traversalDescription()
                .breadthFirst()
                .evaluator(labelEvaluator)
                .evaluator(Evaluators.toDepth(depth))
                .expand(pathExpander)
                .uniqueness(Uniqueness.NODE_GLOBAL);

        try (Transaction tx = db.beginTx()) {
            Node start = db.getNodeById(id);

            for (org.neo4j.graphdb.Path position : td.traverse(start)) {
                Node found = position.endNode();
                results = getProperties(found);
                results.put("neo4j_node_id", found.getId());
                break;
            }

        }

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

}
