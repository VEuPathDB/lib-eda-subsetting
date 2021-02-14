package org.veupathdb.service.eda.ss.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.MapBuilder;
import org.json.JSONArray;

import static org.veupathdb.service.eda.ss.model.RdbmsColumnNames.TT_VARIABLE_ID_COL_NAME;

public class TallRowsGeneratedResultIterator implements Iterator<Map<String,String>> {

  private static final Logger LOG = LogManager.getLogger(TallRowsGeneratedResultIterator.class);

  private final String _entityPkColName;
  private final List<Map<String,String>> _sampleRecord;

  private int _requestedRecordCount;
  private int _deliveredRecordCount;
  private String _currentRecordId;
  private int _nextVariableIndex;

  public TallRowsGeneratedResultIterator(Entity entity, int requestedRecordCount) {
    _entityPkColName = entity.getPKColName();
    _sampleRecord = createSampleRecord(entity);
    _requestedRecordCount = requestedRecordCount;
    _deliveredRecordCount = 0;
    _currentRecordId = "record" + _deliveredRecordCount;
    _nextVariableIndex = 0;
  }

  @Override
  public boolean hasNext() {
    //LOG.info("hasNext() ? " + _deliveredRecordCount + " < " + _requestedRecordCount + " = " + (_deliveredRecordCount < _requestedRecordCount));
    return _deliveredRecordCount < _requestedRecordCount;
  }

  @Override
  public Map<String, String> next() {
    Map<String,String> row = new HashMap<>(_sampleRecord.get(_nextVariableIndex));
    row.put(_entityPkColName, _currentRecordId);
    _nextVariableIndex++;
    if (_nextVariableIndex == _sampleRecord.size()) {
      // reached the end of this record
      _deliveredRecordCount++;
      _currentRecordId = "record" + _deliveredRecordCount;
      _nextVariableIndex = 0;
    }
    //LOG.info("next() returning row: " + FormatUtil.prettyPrint(row, FormatUtil.Style.SINGLE_LINE));
    return row;
  }

  private static List<Map<String, String>> createSampleRecord(Entity entity) {

    Map<String,String> idMap = new HashMap<>();
    for (String colName : entity.getAncestorPkColNames()) {
      idMap.put(colName, "someAncestorId");
    }

    if (entity.getVariables().isEmpty())
      throw new RuntimeException("Test entity must contain variables.");

    List<Map<String,String>> rowList = new ArrayList<>();
    for (Variable var : entity.getVariables()) {
      rowList.add(new MapBuilder<String,String>()
        .putAll(idMap)
        .put(TT_VARIABLE_ID_COL_NAME, var.getId())
        .put(RdbmsColumnNames.VARIABLE_VALUE_COL_NAME, getDefaultValue(var.getType()))
        .toMap());
    }

    LOG.info("Created the following sample record: " + new JSONArray(rowList).toString(2));
    return rowList;
  }

  private static String getDefaultValue(Variable.VariableType type) {
    return switch(type) {
      case DATE -> "2010-01-01T12:00:00";
      case NUMBER -> "123.4567";
      case STRING -> "some fun string";
      case LONGITUDE -> "160.00000";
    };
  }
}
