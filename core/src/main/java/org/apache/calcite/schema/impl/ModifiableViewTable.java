/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.schema.impl;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableIntList;

import java.lang.reflect.Type;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;

/** Extension to {@link ViewTable} that is modifiable. */
public class ModifiableViewTable extends ViewTable
    implements ModifiableView, Wrapper {
  private final Table table;
  private final Path tablePath;
  private final RexNode constraint;
  private final ImmutableIntList columnMapping;
  private final ModifiableViewTableInitializerExpressionFactory initializerExpressionFactory;

  public ModifiableViewTable(Type elementType, RelProtoDataType rowType,
      String viewSql, List<String> schemaPath, List<String> viewPath,
      Table table, Path tablePath, RexNode constraint,
      ImmutableIntList columnMapping, RelDataTypeFactory typeFactory) {
    super(elementType, rowType, viewSql, schemaPath, viewPath);
    this.table = table;
    this.tablePath = tablePath;
    this.constraint = constraint;
    this.columnMapping = columnMapping;
    this.initializerExpressionFactory =
        new ModifiableViewTableInitializerExpressionFactory(typeFactory);
  }

  public RexNode getConstraint(RexBuilder rexBuilder,
      RelDataType tableRowType) {
    return rexBuilder.copy(constraint);
  }

  public ImmutableIntList getColumnMapping() {
    return columnMapping;
  }

  public Table getTable() {
    return table;
  }

  public Path getTablePath() {
    return tablePath;
  }

  @Override public <C> C unwrap(Class<C> aClass) {
    if (aClass.isInstance(initializerExpressionFactory)) {
      return aClass.cast(initializerExpressionFactory);
    }
    return null;
  }

  /**
   * Initialize columns based on the constraint.
   */
  private class ModifiableViewTableInitializerExpressionFactory extends
      NullInitializerExpressionFactory {
    private final RelDataTypeFactory typeFactory;

    private ModifiableViewTableInitializerExpressionFactory(RelDataTypeFactory typeFactory) {
      super(typeFactory);
      this.typeFactory = typeFactory;
    }

    @Override public boolean isGeneratedAlways(RelOptTable table, int iColumn) {
      assert table.unwrap(ModifiableViewTable.class) != null;
      return false;
    }

    @Override public RexNode newColumnDefaultValue(RelOptTable table, int iColumn) {
      final ModifiableViewTable viewTable = table.unwrap(ModifiableViewTable.class);
      final RelDataType viewType = viewTable.getRowType(typeFactory);
      final RexNode constraint = viewTable.getConstraint(rexBuilder, viewType);
      final RelDataType iType = viewType.getFieldList().get(iColumn).getType();

      // Use the view constraint to generate the default value if the column is constrained.
      final RexNode columnConstraint = constraint.accept(new ConstraintVisitor(iColumn, iType));
      if (columnConstraint != null) {
        return columnConstraint;
      }

      // Otherwise use the default value of the underlying table.
      final InitializerExpressionFactory initializerExpressionFactory =
          table.unwrap(InitializerExpressionFactory.class);
      if (initializerExpressionFactory != null) {
        return initializerExpressionFactory.newColumnDefaultValue(table, iColumn);
      }

      return super.newColumnDefaultValue(table, iColumn);
    }

    @Override public RexNode newAttributeInitializer(RelDataType type,
        SqlFunction constructor, int iAttribute, List<RexNode> constructorArgs) {
      throw new UnsupportedOperationException("Not implemented - unknown requirements");
    }

    /**
     * Visit a row constraint to find the column constraint corresponding to a column ordinal.
     */
    private class ConstraintVisitor extends RexVisitorImpl<RexNode> {
      private final Integer ordinal;
      private final RelDataType type;

      private ConstraintVisitor(Integer ordinal, RelDataType type) {
        super(false);
        this.ordinal = ordinal;
        this.type = type;
      }

      @Override public RexNode visitCall(RexCall call) {
        if (!RexUtil.containsInputRef(call)) {
          return null;
        }
        if (EQUALS == call.getOperator()) {
          final RexNode maybeInputRef = call.getOperands().get(0);
          if (maybeInputRef instanceof RexInputRef) {
            if (ordinal == ((RexInputRef) maybeInputRef).getIndex()) {
              return rexBuilder.ensureType(
                  type,
                  call.getOperands().get(1),
                  true);
            }
          }
        } else if (AND == call.getOperator()) {
          for (RexNode maybeCall : call.getOperands()) {
            if (maybeCall instanceof RexCall) {
              final RexNode constraint = visitCall((RexCall) maybeCall);
              if (constraint != null) {
                return constraint;
              }
            }
          }
        }
        return null;
      }
    }
  }
}

// End ModifiableViewTable.java
