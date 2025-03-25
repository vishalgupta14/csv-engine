package org.engine.enums;

public enum JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    CROSS,
    NATURAL;

    public boolean requiresOnCondition() {
        return this != CROSS && this != NATURAL;
    }

    public String getSql() {
        return this.name(); // uppercase for SQL syntax
    }
}


