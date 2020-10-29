package org.veupathdb.service.edass.model;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import static org.gusdb.fgputil.FormatUtil.NL;
import static org.gusdb.fgputil.FormatUtil.TAB;

import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.db.stream.ResultSetIterator;
import org.gusdb.fgputil.functional.TreeNode;
import org.gusdb.fgputil.iterator.GroupingIterator;
import org.gusdb.fgputil.iterator.IteratorUtil;
import org.veupathdb.service.edass.model.Variable.VariableType;

import static org.veupathdb.service.edass.model.RdbmsColumnNames.VARIABLE_ID_COL_NAME;


/**
 * A class to perform subsetting operations on a study entity
 * 
 * @author Steve
 *
 */
public class StudySubsettingUtils {

  private static final String valueColumnName = "value";
  private static final String countColumnName = "count";

  public static void produceTabularSubset(DataSource datasource, Study study, Entity outputEntity,
      List<Variable> outputVariables, List<Filter> filters, OutputStream outputStream) {

    TreeNode<Entity> prunedEntityTree = pruneTree(study.getEntityTree(), filters, outputEntity);

    String sql = generateTabularSql(outputVariables, outputEntity, filters, prunedEntityTree);

    List<String> outputVariableIds = outputVariables.stream().map(v -> v.getId()).collect(Collectors.toList());

    List<String> outputColumns = new ArrayList<>();
    outputColumns.add(outputEntity.getPKColName());
    outputColumns.addAll(outputEntity.getAncestorPkColNames());
    outputColumns.addAll(outputVariableIds);

    new SQLRunner(datasource, sql).executeQuery(rs -> {
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {

        writer.write(String.join("\t", outputColumns) + NL);
        writeWideRows(rs, writer, outputColumns, outputEntity);
        writer.flush();
        return null;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
  
  private static void writeWideRows(ResultSet rs, Writer writer, List<String> outputColumns, Entity outputEntity) throws IOException {

    Iterator<Map<String, String>> tallRowsIterator = new ResultSetIterator<>(rs,
        row -> Optional.of(EntityResultSetUtils.resultSetToTallRowMap(outputEntity, rs, outputColumns)));

    String pkCol = outputEntity.getPKColName();
    Iterator<List<Map<String, String>>> groupedTallRowsIterator = new GroupingIterator<>(
        tallRowsIterator, (row1, row2) -> row1.get(pkCol).equals(row2.get(pkCol)));

    // iterate through groups and format into strings to be written to stream
    for (List<Map<String, String>> group : IteratorUtil.toIterable(groupedTallRowsIterator)) {
      Map<String, String> wideRowMap = EntityResultSetUtils.getTallToWideFunction(outputEntity).apply(group);
      List<String> wideRow = new ArrayList<>();
      for (String colName : outputColumns) {
        wideRow.add(wideRowMap.getOrDefault(colName, ""));
      }
      writer.write(String.join(TAB, wideRow) + NL);
    }
  }
    
  public static void produceVariableDistribution(DataSource datasource, Study study, Entity outputEntity,
      Variable distributionVariable, List<Filter> filters, OutputStream outputStream) {

    // get entity tree pruned to those entities of current interest
    TreeNode<Entity> prunedEntityTree = pruneTree(study.getEntityTree(), filters, outputEntity);

    // get variable count (may do in parallel later)
    int variableCount = getVariableCount(datasource, prunedEntityTree, outputEntity, distributionVariable, filters);
    
    String sql = generateDistributionSql(outputEntity, distributionVariable, filters, prunedEntityTree);
    
    new SQLRunner(datasource, sql).executeQuery(rs -> {
      BufferedOutputStream bufferedOutput = new BufferedOutputStream(outputStream);
      try (JsonGenerator json = new JsonFactory().createGenerator(bufferedOutput, JsonEncoding.UTF8)) {
        json.writeStartObject();
        json.writeNumberField("entitiesCount", variableCount);
        json.writeFieldName("distribution");
        json.writeStartObject();
        while (rs.next()) {
          json.writeNumberField(rs.getString(valueColumnName), rs.getInt(countColumnName));
        }
        json.writeEndObject();
        json.writeEndObject();
        json.flush();
        return null;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }
   
  public static Integer getVariableCount(DataSource datasource, TreeNode<Entity> prunedEntityTree, Entity outputEntity,
      Variable distributionVariable, List<Filter> filters) {
    String sql = generateVariableCountSql(outputEntity, distributionVariable, filters, prunedEntityTree);
    return new SQLRunner(datasource, sql).executeQuery(rs -> {
        rs.next();
        return Integer.valueOf(rs.getInt(countColumnName));
    });
  }

  public static Integer getEntityCount(DataSource datasource, Study study, Entity outputEntity,
      List<Filter> filters) {

    TreeNode<Entity> prunedEntityTree = pruneTree(study.getEntityTree(), filters, outputEntity);
    
    String sql = generateEntityCountSql(outputEntity, filters, prunedEntityTree);
    
    return new SQLRunner(datasource, sql).executeQuery(rs -> {
        rs.next();
        return Integer.valueOf(rs.getInt(countColumnName));
    });
  }

  /**
   * Prune tree to include only active nodes, based on filters and output entity
   * @param tree
   * @param filters
   * @return
   */
  static TreeNode<Entity> pruneTree(TreeNode<Entity> tree, List<Filter> filters, Entity outputEntity) {

    List<String> entityIdsInFilters = getEntityIdsInFilters(filters);

    Predicate<Entity> isActive = e -> entityIdsInFilters.contains(e.getId()) ||
        e.getId().equals(outputEntity.getId());
    return pruneToActiveAndPivotNodes(tree, isActive);
  }
  
  static List<String> getEntityIdsInFilters(List<Filter> filters) {
    return filters.stream().map(f -> f.getEntity().getId()).collect(Collectors.toList());
  }

  /**
   * Generate SQL to produce a multi-column tabular output (the requested variables), for the specified subset.
   * @param outputVariableIds
   * @param outputEntity
   * @param filters
   * @param prunedEntityTree
   * @return
   */
  static String generateTabularSql(List<Variable> outputVariables, Entity outputEntity, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {

    String tallTblAbbrev = "t"; 
    String ancestorTblAbbrev = "a";
    
    return generateWithClauses(prunedEntityTree, filters, getEntityIdsInFilters(filters)) + NL
        + generateTabularSelectClause(outputEntity, tallTblAbbrev, ancestorTblAbbrev) + NL
        + generateTabularFromClause(outputEntity, tallTblAbbrev, ancestorTblAbbrev) + NL
        + generateTabularWhereClause(outputVariables, outputEntity.getPKColName(), tallTblAbbrev, ancestorTblAbbrev) + NL
        + generateInClause(prunedEntityTree, outputEntity, tallTblAbbrev, "AND") + NL
        + generateTabularOrderByClause(outputEntity) + NL;
  }

  /**
   * Generate SQL to produce a multi-column tabular output (the requested variables), for the specified subset.
   * @param outputEntity
   * @param filters
   * @param prunedEntityTree
   * @return
   */
  static String generateEntityCountSql(Entity outputEntity, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {
    
    return generateWithClauses(prunedEntityTree, filters, getEntityIdsInFilters(filters)) + NL
        + "SELECT count(distinct " + outputEntity.getPKColName() + ") as " + countColumnName + NL
        + "FROM (" + NL
        + generateJoiningSubselect(prunedEntityTree, outputEntity) + NL
        + ") t";
  }
  
  /**
   * Generate SQL to produce a distribution for a single variable, for the specified subset.
   * @param outputEntity
   * @param filters
   * @param prunedEntityTree
   * @return
   */
  static String generateDistributionSql(Entity outputEntity, Variable distributionVariable, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {
    
    return generateWithClauses(prunedEntityTree, filters, getEntityIdsInFilters(filters)) + NL
        + generateDistributionSelectClause(distributionVariable) + NL
        + generateDistributionFromClause(outputEntity) + NL
        + generateDistributionWhereClause(distributionVariable) + NL
        + generateInClause(prunedEntityTree, outputEntity, outputEntity.getTallTableName(), "AND") + NL
        + generateDistributionGroupByClause(distributionVariable) + NL
        + "ORDER BY " + valueColumnName + " ASC";
   }
  
  /**
   * Generate SQL to produce a count of the entities that have a value for a variable, for the specified subset.
   * @param outputEntity
   * @param filters
   * @param prunedEntityTree
   * @return
   */
  static String generateVariableCountSql(Entity outputEntity, Variable variable, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {
    
    return generateWithClauses(prunedEntityTree, filters, getEntityIdsInFilters(filters)) + NL
        + generateVariableCountSelectClause(variable) + NL
        + generateDistributionFromClause(outputEntity) + NL
        + generateDistributionWhereClause(variable) + NL
        + generateInClause(prunedEntityTree, outputEntity, outputEntity.getTallTableName(), "AND");        

   }
  static String generateWithClauses(TreeNode<Entity> prunedEntityTree, List<Filter> filters, List<String> entityIdsInFilters) {
    List<String> withClauses = prunedEntityTree.flatten().stream().map(e -> generateWithClause(e, filters)).collect(Collectors.toList());
    return "WITH" + NL
        + String.join("," + NL, withClauses);
  }
  
  /*
   * Get a with clause for this entity.  If the filters don't include any from this entity,
   * then the with clause will just select * from the entity's ancestor table
   */
  static String generateWithClause(Entity entity, List<Filter> filters) {

    List<String> selectColsList = new ArrayList<String>(entity.getAncestorPkColNames());
    selectColsList.add(entity.getPKColName());
    String selectCols = String.join(", ", selectColsList);
    
    // default WITH body assumes no filters. we use the ancestor table because it is small
    String withBody = "  SELECT " + selectCols + " FROM " + entity.getAncestorsTableName() + NL;
    
    List<Filter> filtersOnThisEntity = filters.stream().filter(f -> f.getEntity().getId().equals(entity.getId())).collect(Collectors.toList());

    if (!filtersOnThisEntity.isEmpty()) {
      List<String> filterSqls = filters.stream().filter(f -> f.getEntity().getId().equals(entity.getId())).map(f -> f.getSql()).collect(Collectors.toList());
      withBody = String.join("INTERSECT" + NL, filterSqls);
    } 

    return entity.getWithClauseName() + " as (" + NL + withBody + ")";
  }
  
  static String generateTabularSelectClause(Entity outputEntity, String tallTblAbbrev, String ancestorTblAbbrev) {
    List<String> valColNames = new ArrayList<String>();
    for (VariableType varType : VariableType.values()) valColNames.add(varType.getTallTableColumnName());

    return "SELECT " + outputEntity.getAllPksSelectList(tallTblAbbrev, ancestorTblAbbrev) + ", " + 
    String.join(", ", valColNames);
  }
    
  static String generateDistributionSelectClause(Variable distributionVariable) {
    return "SELECT " + distributionVariable.getType().getTallTableColumnName() + " as " + valueColumnName + ", count(" + distributionVariable.getEntity().getPKColName() + ") as " + countColumnName;
  }
  
  static String generateVariableCountSelectClause(Variable variable) {
    return "SELECT count(distinct " + variable.getEntity().getPKColName() + ") as " + countColumnName;
  }
  
  static String generateDistributionFromClause(Entity outputEntity) {
    return "FROM " + outputEntity.getTallTableName();
  }
  
  static String generateTabularFromClause(Entity outputEntity, String entityTblNm, String ancestorTblNm) {
    return "FROM " + outputEntity.getTallTableName() + " " + entityTblNm + ", " +
        outputEntity.getAncestorsTableName() + " " + ancestorTblNm;
  }
  
  static String generateTabularWhereClause(List<Variable> outputVariables, String entityPkCol, String entityTblNm, String ancestorTblNm) {
    
    List<String> outputVariableIds = outputVariables.stream().map(v -> v.getId()).collect(Collectors.toList());

    List<String> varExprs = new ArrayList<>();
    for (String varId : outputVariableIds) varExprs.add("  " + VARIABLE_ID_COL_NAME + " = '" + varId + "'");
    return "WHERE (" + NL
        + String.join(" OR" + NL, varExprs) + NL
        + ")" + NL
        + "AND " + entityTblNm + "." + entityPkCol + " = " + ancestorTblNm + "." + entityPkCol;
  }
  
  static String generateDistributionWhereClause(Variable outputVariable) {
    return "WHERE " + VARIABLE_ID_COL_NAME + " = '" + outputVariable.getId() + "'";
  }

  static String generateInClause(TreeNode<Entity> prunedEntityTree, Entity outputEntity, String tallTblAbbrev, String whereOrAnd) {
    return whereOrAnd + " " + tallTblAbbrev + "." + outputEntity.getPKColName() + " IN (" + NL
    + generateJoiningSubselect(prunedEntityTree, outputEntity) + NL
    + ")";
  }
  
  static String generateJoiningSubselect(TreeNode<Entity> prunedEntityTree, Entity outputEntity) {
    return generateJoiningSelectClause(outputEntity) + NL
    + generateJoiningFromClause(prunedEntityTree) + NL
    + generateJoiningJoinsClause(prunedEntityTree); 
  }
  
  static String generateJoiningSelectClause(Entity outputEntity) {
    return "  SELECT " + outputEntity.getFullPKColName();
  }
  
  static String generateJoiningFromClause(TreeNode<Entity> prunedEntityTree) {
    List<String> fromClauses = prunedEntityTree.flatten().stream().map(Entity::getWithClauseName).collect(Collectors.toList());
    return "  FROM " + String.join(", ", fromClauses);
  }

  static String generateJoiningJoinsClause(TreeNode<Entity> prunedEntityTree) {
    List<String> sqlJoinStrings = new ArrayList<String>();
    addSqlJoinStrings(prunedEntityTree, sqlJoinStrings);
    return sqlJoinStrings.isEmpty()? "" : "  WHERE " + String.join(NL + "  AND ", sqlJoinStrings);
  }
  
  /*
   * Add to the input list the sql join of a parent entity with each of its children, plus, recursively, its
   * children's sql joins.  (Because the tree might have been pruned, the "parent" is some ancestor)
   */
  static void addSqlJoinStrings(TreeNode<Entity> parent, List<String> sqlJoinStrings) {
    for (TreeNode<Entity> child : parent.getChildNodes()) {
      sqlJoinStrings.add(getSqlJoinString(parent.getContents(), child.getContents()));
      addSqlJoinStrings(child, sqlJoinStrings);
    }
  }

  // this join is formed using the name from the WITH clause, which is the entity name
  static String getSqlJoinString(Entity parentEntity, Entity childEntity) {
    return parentEntity.getWithClauseName() + "." + parentEntity.getPKColName() + " = " +
        childEntity.getWithClauseName() + "." + parentEntity.getPKColName();
  }
  
  static String generateTabularOrderByClause(Entity outputEntity) {
    return "ORDER BY " + outputEntity.getPKColName();
  }
  
  static String generateDistributionGroupByClause(Variable outputVariable) {
    return "GROUP BY " + outputVariable.getType().getTallTableColumnName();
  }
  

  /*
   * PRUNE THE COMPLETE TREE TO JUST THE "ACTIVE" ENTITIES WE WANT FOR OUR JOINS
   * 
   * definition: an active entity is one that must be included in the SQL definition: an active subtree is one
   * in which any entities in the subtree are active.
   * 
   * this entity is active if any of these apply: 1. it has filters 2. it is the output entity 3. it is
   * neither of the above, but has more than one child that is the root of an active subtree
   * 
   * (criterion 3 lets us join elements across connected subtrees)
   * 
   * ----X---- | | --I-- I | | | A I A
   * 
   * In the picture above the A entities are active and I are inactive. X has two children that are active
   * subtrees. We need to force X to be active so that we can join the lower A entities.
   *
   * So will we now have this:
   * 
   * ----A---- | | --I-- I | | | A I A
   * 
   * Finally, we want to prune the tree of inactive nodes, so we have the minimal active tree:
   * 
   * ----A---- | | A A
   * 
   * Now we can ascend the tree and form the concise SQL joins we need
   * 
   * Using a concrete example: ----H---- | | --P-- E | | | O S T
   * 
   * If O and and T are active (have filters or are the output entity), then we ultimately need this join:
   * where O.H_id = H.H_id and T.H_id = H.H_id
   * 
   * (The graceful implementation below is courtesy of Ryan)
   */

  private static TreeNode<Entity> pruneToActiveAndPivotNodes(TreeNode<Entity> root, Predicate<Entity> isActive) {
    return root.mapStructure((nodeContents, mappedChildren) -> {
      List<TreeNode<Entity>> activeChildren = mappedChildren.stream()
        .filter(Objects::nonNull) // filter dead branches
        .collect(Collectors.toList());
      return isActive.test(nodeContents) || activeChildren.size() > 1 ?
        // this node is active itself or a pivot node; return with any active children
        new TreeNode<>(nodeContents).addAllChildNodes(activeChildren) :
        // inactive, non-pivot node; return single active child or null
        activeChildren.isEmpty() ? null : activeChildren.get(0);
    });
  }

}
