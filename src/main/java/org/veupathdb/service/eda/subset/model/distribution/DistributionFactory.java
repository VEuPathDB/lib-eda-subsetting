package org.veupathdb.service.eda.subset.model.distribution;

import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.Tuples;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
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

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class DistributionFactory {

  private static class EdaDistributionStreamProvider<T extends VariableWithValues<?>> implements DistributionStreamProvider {

    // used to produce the stream of distribution tuples
    private final DatabaseInstance _db;
    private final String _appDbSchema;
    private final Entity _targetEntity;
    protected final T _variable;
    private final List<Filter> _filters;

    private final TreeNode<Entity> _prunedEntityTree;
    private final long _subsetEntityCount;

    public EdaDistributionStreamProvider(
        DatabaseInstance db, String appDbSchema, Study study, Entity targetEntity, T variable, List<Filter> filters) {
      _db = db;
      _appDbSchema = appDbSchema;
      _targetEntity = targetEntity;
      _variable = variable;
      _filters = filters;

      // get entity tree pruned to those entities of current interest
      _prunedEntityTree = FilteredResultFactory.pruneTree(
          study.getEntityTree(), _filters, _targetEntity);

      // get the number of entities in the subset
      _subsetEntityCount = FilteredResultFactory.getEntityCount(
          _db.getDataSource(), _appDbSchema, _prunedEntityTree, _targetEntity, _filters);
    }

    @Override
    public Stream<Tuples.TwoTuple<String, Long>> getDistributionStream() {
      return FilteredResultFactory.produceVariableDistribution(
          _db, _appDbSchema, _prunedEntityTree, _targetEntity, _variable, _filters);
    }

    @Override
    public long getRecordCount() {
      return _subsetEntityCount;
    }
  }

  public static DistributionResult processDistributionRequest(
      DatabaseInstance db, String appDbSchema, Study study, Entity targetEntity,
      VariableWithValues<?> var, List<Filter> filters,
      ValueSpec apiValueSpec, Optional<BinSpecWithRange> incomingBinSpec) {
    try {
      AbstractDistribution.ValueSpec valueSpec = convertValueSpec(apiValueSpec);

      // inspect requested variable and select appropriate distribution
      AbstractDistribution distribution;
      if (var.getDataShape() == VariableDataShape.CONTINUOUS) {
        distribution = switch (var.getType()) {
          case INTEGER -> new IntegerBinDistribution(
            new EdaDistributionStreamProvider<>(db, appDbSchema, study, targetEntity, (IntegerVariable) var, filters),
            valueSpec, new EdaNumberBinSpec((IntegerVariable) var, incomingBinSpec)
          );
          case NUMBER -> new FloatingPointBinDistribution(
            new EdaDistributionStreamProvider<>(db, appDbSchema, study, targetEntity, (FloatingPointVariable) var, filters),
            valueSpec, new EdaNumberBinSpec((FloatingPointVariable) var, incomingBinSpec)
          );
          case DATE -> new DateBinDistribution(
            new EdaDistributionStreamProvider<>(db, appDbSchema, study, targetEntity, (DateVariable) var, filters),
            valueSpec, new EdaDateBinSpec((DateVariable) var, incomingBinSpec)
          );
          default -> throw new BadRequestException("Among continuous variables, " +
            "distribution endpoint supports only date, integer, and number types; " +
            "requested variable '" + var.getId() + "' is type " + var.getType());
        };
      }
      else {
        if (incomingBinSpec.isPresent()) {
          throw new BadRequestException("Bin spec is allowed/required only for continuous variables.");
        }
        distribution = new DiscreteDistribution(
            new EdaDistributionStreamProvider<>(
                db, appDbSchema, study, targetEntity, var, filters), valueSpec);
      }

      return distribution.generateDistribution();
    }
    catch(IllegalArgumentException e) {
      // underlying lib sometimes throws this; indicates bad request data coming in
      throw new BadRequestException("Variable " + var.getId() + " of type " + var.getType() + ": " + e.getMessage(), e);
    }
  }

  private static AbstractDistribution.ValueSpec convertValueSpec(ValueSpec apiValueSpec) {
    return switch (apiValueSpec) {
      case COUNT -> AbstractDistribution.ValueSpec.COUNT;
      case PROPORTION -> AbstractDistribution.ValueSpec.PROPORTION;
    };
  }
}
