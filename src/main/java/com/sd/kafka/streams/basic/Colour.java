package com.sd.kafka.streams.basic;
public enum Colour {
    RED("1", "Red Colour"),
    GREEN("2", "Green Colour"),
    BLUE("3", "Blue Colour");

    private final String id;
    private final String displayName;

    Colour(String id, String displayName) {
        this.id = id;
        this.displayName = displayName;
    }

    public String getId() {
        return id;
    }

    public String getDisplayName() {
        return displayName;
    }
}