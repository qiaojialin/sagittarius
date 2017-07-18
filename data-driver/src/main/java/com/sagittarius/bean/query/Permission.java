package com.sagittarius.bean.query;

/**
 * Created by Leah on 2017/7/18.
 */
public enum Permission {
    READ("READ"), WRITE("WRITE"), READWRITE("READWRITE");

    private String _name;
    Permission(String name){
        this._name = name;
    }

    public String toString(){
        return this._name;
    }
}
