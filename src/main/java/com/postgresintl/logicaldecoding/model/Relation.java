package com.postgresintl.logicaldecoding.model;

import java.util.ArrayList;
import java.util.List;

public class Relation {
    int oid;
    String schema;
    String name;
    List <Attribute> attributes = new ArrayList<Attribute>();

    public int getOid() {
        return oid;
    }

    public void setOid(int oid) {
        this.oid = oid;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    public void addAttribute(Attribute attribute){
        attributes.add(attribute);
    }
    @Override
    public String toString(){
        return ""+schema+'.'+name + attributes;
    }
}
