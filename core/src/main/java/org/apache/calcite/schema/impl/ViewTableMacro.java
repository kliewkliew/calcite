package org.apache.calcite.schema.impl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;

/** Table function that implements a view. It returns the operator
 * tree of the view's SQL query. */
public class ViewTableMacro implements TableMacro {
  protected final String viewSql;
  protected final CalciteSchema schema;
  private final Boolean modifiable;
  /** Typically null. If specified, overrides the path of the schema as the
   * context for validating {@code viewSql}. */
  protected final List<String> schemaPath;
  protected final List<String> viewPath;

  public ViewTableMacro(CalciteSchema schema, String viewSql, List<String> schemaPath,
      List<String> viewPath, Boolean modifiable) {
    this.viewSql = viewSql;
    this.schema = schema;
    this.viewPath = viewPath == null ? null : ImmutableList.copyOf(viewPath);
    this.modifiable = modifiable;
    this.schemaPath =
        schemaPath == null ? null : ImmutableList.copyOf(schemaPath);
  }

  public List<FunctionParameter> getParameters() {
    return Collections.emptyList();
  }

  public TranslatableTable apply(List<Object> arguments) {
    CalcitePrepare.AnalyzeViewResult parsed =
        Schemas.analyzeView(MaterializedViewTable.MATERIALIZATION_CONNECTION,
            schema, schemaPath, viewSql, modifiable != null && modifiable);
    final List<String> schemaPath1 =
        schemaPath != null ? schemaPath : schema.path(null);
    final JavaTypeFactory typeFactory = (JavaTypeFactory) parsed.typeFactory;
    final Type elementType = typeFactory.getJavaClass(parsed.rowType);
    if ((modifiable == null || modifiable) && parsed.table != null) {
      return modifiableViewTable(parsed, viewSql, schemaPath1, viewPath, schema);
    } else {
      return viewTable(parsed, viewSql, schemaPath1, viewPath);
    }
  }

  /**
   * Allow a subclass to return an extension of ModifiableViewTable by overriding this method.
   */
  protected ModifiableViewTable modifiableViewTable(CalcitePrepare.AnalyzeViewResult parsed,
      String viewSql, List<String> schemaPath, List<String> viewPath, CalciteSchema schema) {
    final JavaTypeFactory typeFactory = (JavaTypeFactory) parsed.typeFactory;
    final Type elementType = typeFactory.getJavaClass(parsed.rowType);
    return new ModifiableViewTable(elementType,
        RelDataTypeImpl.proto(parsed.rowType), viewSql, schemaPath, viewPath,
        parsed.table, Schemas.path(schema.root(), parsed.tablePath),
        parsed.constraint, parsed.columnMapping, parsed.typeFactory);
  }

  /**
   * Allow a subclass to return an extension of ViewTable by overriding this method.
   */
  protected ViewTable viewTable(CalcitePrepare.AnalyzeViewResult parsed, String viewSql,
      List<String> schemaPath, List<String> viewPath) {
    final JavaTypeFactory typeFactory = (JavaTypeFactory) parsed.typeFactory;
    final Type elementType = typeFactory.getJavaClass(parsed.rowType);
    return new ViewTable(elementType,
        RelDataTypeImpl.proto(parsed.rowType), viewSql, schemaPath, viewPath);
  }
}
