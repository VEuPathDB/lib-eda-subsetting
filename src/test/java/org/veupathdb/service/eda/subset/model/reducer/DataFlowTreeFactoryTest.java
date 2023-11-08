package org.veupathdb.service.eda.subset.model.reducer;

import org.gusdb.fgputil.functional.TreeNode;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.testutil.TestDataProvider;

import java.util.Collections;

public class DataFlowTreeFactoryTest {

  @Test
  public void testLeafOutputNode() {
    DataFlowTreeFactory factory = new DataFlowTreeFactory();
    TreeNode<Entity> root = new TreeNode<>(new TestDataProvider.EntityBuilder()
        .withEntityId("Root")
        .build());
    TreeNode<Entity> middle = new TreeNode<>(new TestDataProvider.EntityBuilder()
        .withEntityId("Middle")
        .build());
    TreeNode<Entity> leaf = new TreeNode<>(new TestDataProvider.EntityBuilder()
        .withEntityId("Leaf")
        .build());
    root.addChildNode(middle);
    middle.addChildNode(leaf);
    TreeNode<DataFlowNodeContents> newRoot = factory.create(root, leaf.getContents(), Collections.emptyList(), Collections.emptyList(), null);
    MatcherAssert.assertThat(newRoot.getChildNodes(), Matchers.hasSize(1));
    TreeNode<DataFlowNodeContents> newMiddle = newRoot.getChildNodes().get(0);
    MatcherAssert.assertThat(newMiddle.getChildNodes(), Matchers.hasSize(1));
    TreeNode<DataFlowNodeContents> newLeaf = newMiddle.getChildNodes().get(0);
    MatcherAssert.assertThat(newLeaf.getChildNodes(), Matchers.empty());

    // Assert middle node is still in the middle and that the new leaf node has the contents of the old root.
    Assertions.assertEquals(middle.getContents(), newMiddle.getContents().getEntity());
    Assertions.assertEquals(root.getContents(), newLeaf.getContents().getEntity());
  }
}
