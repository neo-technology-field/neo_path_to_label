package com.maxdemarzi;

import org.neo4j.graphdb.RelationshipType;

public enum RelationshipTypes  implements RelationshipType {
    ds_rel,
    sample,
    member,
    has,
    administers,
    failed_by,
    selected_by,
    passed_by
}
