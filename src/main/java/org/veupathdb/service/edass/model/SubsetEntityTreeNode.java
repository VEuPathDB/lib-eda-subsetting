package org.veupathdb.service.edass.model;

import java.util.List;

import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.functional.TreeNode;

public class SubsetEntityTreeNode extends TreeNode<SubsetEntity> {

  public SubsetEntityTreeNode(SubsetEntity subsetEntity) {
    super(subsetEntity);
  }

  /*
   * Validate new child before adding
   */
  public TreeNode<SubsetEntity> addChild(SubsetEntity child) {
    String errPrefix = "In entity " + getContents().getEntityId() + ", adding a child with the same ID as ";

    if (child.getEntityId().equals(getContents().getEntityId()))
      throw new InternalServerErrorException(errPrefix + "the parent: " + child.getEntityId());

    if (getChildNodes().stream()
        .filter(ch -> ch.getContents().getEntityId() == child.getEntityId()).count() != 0) 
      throw new InternalServerErrorException(errPrefix + "another child: " + child.getEntityId());
    
    return super.addChild(child);
  }  
  
  /*
   * Add to the input list the sql join of this entity with each of its children, 
   * plus, recursively, its children's sql joins
   */
  void addSqlJoinStrings(List<String> sqlJoinStrings) {
        
    for (TreeNode<SubsetEntity> child: getChildNodes()) {
      
      sqlJoinStrings.add(getContents().getSqlJoinString(child.getContents()));
      
      ((SubsetEntityTreeNode)child).addSqlJoinStrings(sqlJoinStrings);
    }
  }
}
