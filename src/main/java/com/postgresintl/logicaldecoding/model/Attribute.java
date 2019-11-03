package com.postgresintl.logicaldecoding.model;

public class Attribute {
    int oid;
    String name;
    int mode;
    byte identityFull;

    public Attribute(int attrId, String attrName, int attrMode, byte replicaIdentityFull) {
        oid = attrId;
        name = attrName;
        mode = attrMode;
        identityFull = replicaIdentityFull;
    }
    public String toString(){
        return "oid:" + oid + " name:" + name + " mode:" + mode + " identityFull:" + identityFull;
    }
}
