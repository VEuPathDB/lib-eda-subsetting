package org.veupathdb.service.edass.model;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.sql.DataSource;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.edass.model.Variable.VariableType;

/**
 * A class to perform subsetting operations on a study entity
 * 
 * @author Steve
 *
 */
public class StudySubsettingUtils {

  private static final String nl = System.lineSeparator();
  
  private static final String ontologyTermName = "ontology_term_name";

  public static void produceTabularSubset(DataSource datasource, Study study, Entity outputEntity,
      List<String> outputVariableNames, List<Filter> filters) {

    TreeNode<Entity> prunedEntityTree = pruneTree(study.getEntityTree(), filters, outputEntity);
    
    String sql = generateTabularSql(outputVariableNames, outputEntity, filters, prunedEntityTree);

    // TODO run sql and produce stream output
  }
  
  
  public static void produceHistogramSubset(DataSource datasource, Study study, Entity outputEntity,
      Variable histogramVariable, List<Filter> filters) {

    TreeNode<Entity> prunedEntityTree = pruneTree(study.getEntityTree(), filters, outputEntity);
    
    String sql = generateHistogramSql(outputEntity, histogramVariable, filters, prunedEntityTree);
    // TODO run sql and produce stream output
  }

  /**
   * Prune tree to include only active nodes, based on filters and output entity
   * @param tree
   * @param filters
   * @return
   */
  static TreeNode<Entity> pruneTree(TreeNode<Entity> tree, List<Filter> filters, Entity outputEntity) {

    List<String> entityIdsInFilters = getEntityIdsInFilters(filters);

    Predicate<Entity> isActive = e -> entityIdsInFilters.contains(e.getEntityId()) ||
        e.getEntityId().equals(outputEntity.getEntityId());
    return pruneToActiveAndPivotNodes(tree, isActive);
  }
  
  static List<String> getEntityIdsInFilters(List<Filter> filters) {
    return filters.stream().map(f -> f.getEntity().getEntityId()).collect(Collectors.toList());
  }

  /**
   * Generate SQL to produce a multi-column tabular output (the requested variables), for the specified subset.
   * @param outputVariableNames
   * @param outputEntity
   * @param filters
   * @param prunedEntityTree
   * @param entityIdsInFilters
   * @return
   */
  static String generateTabularSql(List<String> outputVariableNames, Entity outputEntity, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {

    String tallTblAbbrev = "t"; 
    String ancestorTblAbbrev = "a";
    
    return generateWithClauses(prunedEntityTree, filters, getEntityIdsInFilters(filters)) + nl
        + generateTabularSelectClause(outputEntity, tallTblAbbrev, ancestorTblAbbrev) + nl
        + generateTabularFromClause(outputEntity, tallTblAbbrev, ancestorTblAbbrev) + nl
        + generateTabularWhereClause(outputVariableNames, outputEntity.getEntityPKColName(), tallTblAbbrev, ancestorTblAbbrev) + nl
        + generateInClause(prunedEntityTree, outputEntity, tallTblAbbrev) + nl
        + generateTabularOrderByClause(outputEntity) + nl;
  }

  /**
   * Generate SQL to produce a histogram for a single variable, for the specified subset.
   * @param outputVariableNames
   * @param outputEntity
   * @param filters
   * @param prunedEntityTree
   * @param entityIdsInFilters
   * @return
   */
  static String generateHistogramSql(Entity outputEntity, Variable histogramVariable, List<Filter> filters, TreeNode<Entity> prunedEntityTree) {
    
    List<String> outputVariableNames = new ArrayList<String>();
    outputVariableNames.add(outputEntity.getEntityPKColName());
    
    return generateWithClauses(prunedEntityTree, filters, getEntityIdsInFilters(filters)) + nl
        + generateHistogramSelectClause(histogramVariable) + nl
        + generateHistogramFromClause(outputEntity) + nl
        + generateHistogramWhereClause(histogramVariable) + nl
        + generateInClause(prunedEntityTree, outputEntity, outputEntity.getEntityTallTableName()) + nl        
        + generateHistogramGroupByClause(histogramVariable) + nl;
   }
  
  static String generateWithClauses(TreeNode<Entity> prunedEntityTree, List<Filter> filters, List<String> entityIdsInFilters) {
    List<String> withClauses = prunedEntityTree.flatten().stream().map(e -> generateWithClause(e, filters)).collect(Collectors.toList());
    return "WITH" + nl
        + String.join("," + nl, withClauses);
  }
  
  /*
   * Get a with clause for this entity.  If the filters don't include any from this entity,
   * then the with clause will just select * from the entity's ancestor table
   */
  static String generateWithClause(Entity entity, List<Filter> filters) {

    List<String> selectColsList = new ArrayList<String>(entity.getAncestorPkColNames());
    selectColsList.add(entity.getEntityPKColName());
    String selectCols = String.join(", ", selectColsList);
    
    // default WITH body assumes no filters. we use the ancestor table because it is small
    String withBody = "  SELECT " + selectCols + " FROM " + entity.getEntityAncestorsTableName() + nl;
    
    List<Filter> filtersOnThisEnity = filters.stream().filter(f -> f.getEntity().getEntityId().equals(entity.getEntityId())).collect(Collectors.toList());

    if (!filtersOnThisEnity.isEmpty()) {
      List<String> filterSqls = filters.stream().filter(f -> f.getEntity().getEntityId().equals(entity.getEntityId())).map(f -> f.getSql()).collect(Collectors.toList());
      withBody = String.join("INTERSECT" + nl, filterSqls);
    } 

    return entity.getEntityName() + " as (" + nl + withBody + ")";
  }
  
  static String generateTabularSelectClause(Entity outputEntity, String tallTblAbbrev, String ancestorTblAbbrev) {
    List<String> valColNames = new ArrayList<String>();
    for (VariableType varType : VariableType.values()) valColNames.add(varType.getTallTableColumnName());

    return "SELECT " + outputEntity.getAllPksSelectList(tallTblAbbrev, ancestorTblAbbrev) + ", " + 
    String.join(", ", valColNames);
  }
    
  static String generateHistogramSelectClause(Variable histogramVariable) {
    return "SELECT count(" + histogramVariable.getName() + "), " + histogramVariable.getVariableType().getTallTableColumnName();
  }
  
  static String generateHistogramFromClause(Entity outputEntity) {
    return "FROM " + outputEntity.getEntityTallTableName();
  }
  
  static String generateTabularFromClause(Entity outputEntity, String entityTblNm, String ancestorTblNm) {
    return "FROM " + outputEntity.getEntityTallTableName() + " " + entityTblNm + ", " +
        outputEntity.getEntityAncestorsTableName() + " " + ancestorTblNm;
  }
  
  static String generateTabularWhereClause(List<String> outputVariableNames, String entityPkCol, String entityTblNm, String ancestorTblNm) {
    
    List<String> varExprs = new ArrayList<String>();
    for (String varName : outputVariableNames) varExprs.add("  " + ontologyTermName + " = '" + varName + "'");
    return "WHERE (" + nl 
        + String.join(" OR" + nl, varExprs) + nl
        + ")" + nl
        + "AND " + entityTblNm + "." + entityPkCol + " = " + ancestorTblNm + "." + entityPkCol;
  }
  
  static String generateHistogramWhereClause(Variable outputVariable) {
    return "WHERE ontology_term_name = '" + outputVariable.getName() + "'";
  }

  static String generateInClause(TreeNode<Entity> prunedEntityTree, Entity outputEntity, String tallTblAbbrev) {
    return "AND " + tallTblAbbrev + "." + outputEntity.getEntityPKColName() + " IN (" + nl 
    + generateInClauseSelectClause(outputEntity) + nl
    + generateInClauseFromClause(prunedEntityTree) + nl
    + generateInClauseJoinsClause(prunedEntityTree) + nl
    + ")";

  }
  
  static String generateInClauseSelectClause(Entity outputEntity) {
    return "  SELECT " + outputEntity.getEntityFullPKColName();
  }
  
  static String generateInClauseFromClause(TreeNode<Entity> prunedEntityTree) {
    List<String> fromClauses = prunedEntityTree.flatten().stream().map(e -> e.getEntityName()).collect(Collectors.toList());
    return "  FROM " + String.join(", ", fromClauses);
  }

  static String generateInClauseJoinsClause(TreeNode<Entity> prunedEntityTree) {
    List<String> sqlJoinStrings = new ArrayList<String>();
    addSqlJoinStrings(prunedEntityTree, sqlJoinStrings);
    return "  WHERE " + String.join(nl + "  AND ", sqlJoinStrings);    
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
    return parentEntity.getEntityName() + "." + parentEntity.getEntityPKColName() + " = " +
        childEntity.getEntityName() + "." + parentEntity.getEntityPKColName();
  }
  
  static String generateTabularOrderByClause(Entity outputEntity) {
    return "ORDER BY " + outputEntity.getEntityPKColName();
  }
  
  static String generateHistogramGroupByClause(Variable outputVariable) {
    return "GROUP BY " + outputVariable.getVariableType().getTallTableColumnName();
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

  private static <T> TreeNode<T> pruneToActiveAndPivotNodes(TreeNode<T> root, Predicate<T> isActive) {
    return root.mapStructure((nodeContents, mappedChildren) -> {
      List<TreeNode<T>> activeChildren = mappedChildren.stream().filter(child -> child != null) // filter dead
                                                                                                // branches
          .collect(Collectors.toList());
      return isActive.test(nodeContents) || activeChildren.size() > 1 ?
      // this node is active itself or a pivot node; return with any active children
      new TreeNode<T>(nodeContents).addAllChildNodes(activeChildren) :
      // inactive, non-pivot node; return single active child or null
      activeChildren.isEmpty() ? null : activeChildren.get(0);
    });
  }

}
