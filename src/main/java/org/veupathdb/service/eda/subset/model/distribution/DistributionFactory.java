package org.veupathdb.service.eda.subset.model.distribution;

import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.Tuples;
import org.gusdb.fgputil.distribution.*;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.db.FilteredResultFactory;
import org.veupathdb.service.eda.subset.model.filter.Filter;
import org.veupathdb.service.eda.subset.model.variable.DateVariable;
import org.veupathdb.service.eda.subset.model.variable.FloatingPointVariable;
import org.veupathdb.service.eda.subset.model.variable.IntegerVariable;
import org.veupathdb.service.eda.subset.model.variable.VariableDataShape;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class DistributionFactory {

  private static class EdaDistributionStreamProvider<T extends VariableWithValues> implements DistributionStreamProvider {

    // used to produce the stream of distribution tuples
    private final DataSource _ds;
    private final String _appDbSchema;
    private final Entity _targetEntity;
    protected final T _variable;
    private final List<Filter> _filters;

    private final TreeNode<Entity> _prunedEntityTree;
    private final long _subsetEntityCount;

    public EdaDistributionStreamProvider(
        DataSource ds, String appDbSchema, Study study, Entity targetEntity, T variable, List<Filter> filters) {
      _ds = ds;
      _appDbSchema = appDbSchema;
      _targetEntity = targetEntity;
      _variable = variable;
      _filters = filters;

      // get entity tree pruned to those entities of current interest
      _prunedEntityTree = FilteredResultFactory.pruneTree(
          study.getEntityTree(), _filters, _targetEntity);

      // get the number of entities in the subset
      _subsetEntityCount = FilteredResultFactory.getEntityCount(
          _ds, _appDbSchema, _prunedEntityTree, _targetEntity, _filters);
    }

    @Override
    public Stream<Tuples.TwoTuple<String, Long>> getDistributionStream() {
      return FilteredResultFactory.produceVariableDistribution(
          _ds, _appDbSchema, _prunedEntityTree, _targetEntity, _variable, _filters);
    }

    @Override
    public long getRecordCount() {
      return _subsetEntityCount;
    }
  }

  public static DistributionResult processDistributionRequest(
      DataSource ds, String appDbSchema, Study study, Entity targetEntity,
      VariableWithValues var, List<Filter> filters,
      ValueSpec apiValueSpec, Optional<BinSpecWithRange> incomingBinSpec) {
    try {
      AbstractDistribution.ValueSpec valueSpec = convertValueSpec(apiValueSpec);

      // inspect requested variable and select appropriate distribution
      AbstractDistribution distribution;
      if (var.getDataShape() == VariableDataShape.CONTINUOUS) {
        switch(var.getType()) {
          case INTEGER:
            distribution = new IntegerBinDistribution(
              new EdaDistributionStreamProvider<>(ds, appDbSchema, study, targetEntity, (IntegerVariable)var, filters),
              valueSpec, new EdaNumberBinSpec((IntegerVariable)var, incomingBinSpec));
            break;
          case NUMBER:
            distribution = new FloatingPointBinDistribution(
              new EdaDistributionStreamProvider<>(ds, appDbSchema, study, targetEntity, (FloatingPointVariable)var, filters),
              valueSpec, new EdaNumberBinSpec((FloatingPointVariable)var, incomingBinSpec));
            break;
          case DATE:
            distribution = new DateBinDistribution(
              new EdaDistributionStreamProvider<>(ds, appDbSchema, study, targetEntity, (DateVariable)var, filters),
              valueSpec, new EdaDateBinSpec((DateVariable)var, incomingBinSpec));
            break;
          default: throw new BadRequestException("Among continuous variables, " +
              "distribution endpoint supports only date, integer, and number types; " +
              "requested variable '" + var.getId() + "' is type " + var.getType());
        }
      }
      else {
        if (incomingBinSpec.isPresent()) {
          throw new BadRequestException("Bin spec is allowed/required only for continuous variables.");
        }
        distribution = new DiscreteDistribution(
            new EdaDistributionStreamProvider<>(
                ds, appDbSchema, study, targetEntity, var, filters), valueSpec);
      }

      return distribution.generateDistribution();
    }
    catch(IllegalArgumentException e) {
      // underlying lib sometimes throws this; indicates bad request data coming in
      throw new BadRequestException("Variable " + var.getId() + " of type " + var.getType() + ": " + e.getMessage(), e);
    }
  }

  private static AbstractDistribution.ValueSpec convertValueSpec(ValueSpec apiValueSpec) {
    switch(apiValueSpec) {
      case COUNT: return AbstractDistribution.ValueSpec.COUNT;
      case PROPORTION: return AbstractDistribution.ValueSpec.PROPORTION;
      default: throw new IllegalArgumentException();
    }
  }
}
