package org.engine.entity;

import org.engine.db.processor.CsvDbLoader;
import org.engine.enums.JoinType;

public class JoinTarget {
    public final CsvDbLoader table;
    public final JoinType joinType;
    public final String joinCondition;

    public JoinTarget(CsvDbLoader table, JoinType joinType, String joinCondition) {
        this.table = table;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
    }
}
