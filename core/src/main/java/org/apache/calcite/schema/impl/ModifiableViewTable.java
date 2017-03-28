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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.Path;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.validate.SqlValidatorUtil.mapNameToIndex;

/** Extension to {@link ViewTable} that is modifiable. */
public class ModifiableViewTable extends ViewTable
    implements ModifiableView, Wrapper {
  private final Table table;
  private final Path tablePath;
  private final RexNode constraint;
  private final ImmutableIntList columnMapping;
  private final InitializerExpressionFactory initializerExpressionFactory;

  /** Creates a ModifiableViewTable. */
  public ModifiableViewTable(Type elementType, RelProtoDataType rowType,
      String viewSql, List<String> schemaPath, List<String> viewPath,
      Table table, Path tablePath, RexNode constraint,
      ImmutableIntList columnMapping) {
    super(elementType, rowType, viewSql, schemaPath, viewPath);
    this.table = table;
    this.tablePath = tablePath;
    this.constraint = constraint;
    this.columnMapping = columnMapping;
    this.initializerExpressionFactory = new ModifiableViewTableInitializerExpressionFactory();
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
    } else if (aClass.isInstance(table)) {
      return aClass.cast(table);
    }
    return null;
  }

  /**
   * Extends the underlying table and returns a new view with updated row-type and column-mapping.
   */
  public final ModifiableViewTable extend(
      List<RelDataTypeField> fields, RelDataTypeFactory typeFactory) {
    final ExtensibleTable underlying = unwrap(ExtensibleTable.class);
    assert underlying != null;

    final RelDataType oldRowType = getRowType(typeFactory);
    final Iterable<RelDataTypeField> allFields =
        Iterables.unmodifiableIterable(Iterables.concat(oldRowType.getFieldList(), fields));

    // Find the set of extended fields that do not duplicate a view column.
    final LinkedHashMap<String, RelDataTypeField> dedupedFieldsMap =
        Maps.newLinkedHashMapWithExpectedSize(oldRowType.getFieldCount() + fields.size());
    for (RelDataTypeField field : allFields) {
      if (!dedupedFieldsMap.containsKey(field.getName())) {
        dedupedFieldsMap.put(field.getName(), field);
      }
    }

    // The characteristics of the new view.
    final ImmutableList<RelDataTypeField> dedupedFields =
        ImmutableList.copyOf(dedupedFieldsMap.values());
    final RelDataType newRowType = typeFactory.createStructType(dedupedFields);
    final ImmutableList<RelDataTypeField> dedupedExtendedFields =
        dedupedFields.subList(getColumnMapping().size(), dedupedFields.size());
    final ImmutableIntList newColumnMapping =
        getNewColumnMapping(underlying, getColumnMapping(), dedupedExtendedFields, typeFactory);

    // Extend the underlying table with only the fields that
    // duplicate columns in neither the view nor the base table.
    final List<RelDataTypeField> columnsForExtendingBaseTable =
        getExtendedColumnsForBaseTable(underlying, dedupedExtendedFields, typeFactory);
    final Table extendedTable = underlying.extend(columnsForExtendingBaseTable);

    return extend(extendedTable, newRowType, newColumnMapping);
  }

  private static List<RelDataTypeField> getExtendedColumnsForBaseTable(
      ExtensibleTable underlying, List<RelDataTypeField> extendedColumns,
      RelDataTypeFactory typeFactory) {
    final List<RelDataTypeField> baseColumns = underlying.getRowType(typeFactory).getFieldList();
    final Map<String, Integer> nameToIndex = mapNameToIndex(baseColumns);

    final ImmutableList.Builder<RelDataTypeField> outputCols =
        ImmutableList.builder();
    for (RelDataTypeField extendedColumn : extendedColumns) {
      if (!nameToIndex.containsKey(extendedColumn.getName())) {
        outputCols.add(extendedColumn);
      }
    }
    return outputCols.build();
  }

  /**
   * Creates a mapping from the view index to the index in the underlying table.
   */
  private static ImmutableIntList getNewColumnMapping(ExtensibleTable underlying,
      ImmutableIntList oldColumnMapping, ImmutableList<RelDataTypeField> extendedColumns,
      RelDataTypeFactory typeFactory) {
    final List<RelDataTypeField> baseColumns = underlying.getRowType(typeFactory).getFieldList();
    final Map<String, Integer> nameToIndex = mapNameToIndex(baseColumns);

    final ImmutableList.Builder<Integer> newMapping = ImmutableList.builder();
    newMapping.addAll(oldColumnMapping);
    int newMappedIndex = oldColumnMapping.size();
    for (RelDataTypeField extendedColumn : extendedColumns) {
      if (nameToIndex.containsKey(extendedColumn.getName())) {
        // The extended column duplicates a column in the underlying table.
        newMapping.add(nameToIndex.get(extendedColumn.getName()));
      } else {
        // The extended column in not in the underlying table.
        newMapping.add(newMappedIndex++);
      }
    }
    return ImmutableIntList.copyOf(newMapping.build());
  }

  protected ModifiableViewTable extend(
      Table extendedTable, RelDataType newRowType, ImmutableIntList newColumnMapping) {
    return new ModifiableViewTable(getElementType(), RelDataTypeImpl.proto(newRowType),
        getViewSql(), getSchemaPath(), getViewPath(), extendedTable, getTablePath(), constraint,
        newColumnMapping);
  }

  /**
   * Initializes columns based on the view constraint.
   */
  private class ModifiableViewTableInitializerExpressionFactory
      extends NullInitializerExpressionFactory {
    private final ImmutableMap<Integer, RexNode> projectMap;

    private ModifiableViewTableInitializerExpressionFactory() {
      super();
      final Map<Integer, RexNode> projectMap = Maps.newHashMap();
      final List<RexNode> filters = new ArrayList<>();
      RelOptUtil.inferViewPredicates(projectMap, filters, constraint);
      assert filters.isEmpty();
      this.projectMap = ImmutableMap.copyOf(projectMap);
    }

    @Override public boolean isGeneratedAlways(RelOptTable table, int iColumn) {
      assert table.unwrap(ModifiableViewTable.class) != null;
      return false;
    }

    @Override public RexNode newColumnDefaultValue(RelOptTable table, int iColumn,
        RexBuilder rexBuilder) {
      final ModifiableViewTable viewTable = table.unwrap(ModifiableViewTable.class);
      assert iColumn < viewTable.columnMapping.size();
      final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      final RelDataType viewType = viewTable.getRowType(typeFactory);
      final RelDataType iType = viewType.getFieldList().get(iColumn).getType();

      // Use the view constraint to generate the default value if the column is constrained.
      final int mappedOrdinal = viewTable.columnMapping.get(iColumn);
      final RexNode viewConstraint = projectMap.get(mappedOrdinal);
      if (viewConstraint != null) {
        return rexBuilder.ensureType(iType, viewConstraint, true);
      }

      // Otherwise use the default value of the underlying table.
      final Table schemaTable = viewTable.unwrap(Table.class);
      if (schemaTable instanceof Wrapper) {
        final InitializerExpressionFactory initializerExpressionFactory =
            ((Wrapper) schemaTable).unwrap(InitializerExpressionFactory.class);
        if (initializerExpressionFactory != null) {
          final RexNode tableConstraint =
              initializerExpressionFactory.newColumnDefaultValue(table, iColumn, rexBuilder);
          return rexBuilder.ensureType(iType, tableConstraint, true);
        }
      }

      // Otherwise Sql type of NULL.
      return super.newColumnDefaultValue(table, iColumn, rexBuilder);
    }

    @Override public RexNode newAttributeInitializer(RelDataType type, SqlFunction constructor,
        int iAttribute, List<RexNode> constructorArgs, RexBuilder rexBuilder) {
      throw new UnsupportedOperationException("Not implemented - unknown requirements");
    }
  }
}

// End ModifiableViewTable.java
