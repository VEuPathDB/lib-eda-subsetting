package org.veupathdb.service.edass.service;

import org.gusdb.fgputil.db.runner.SQLRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;
import javax.ws.rs.InternalServerErrorException;

import org.veupathdb.service.edass.Resources;
import org.veupathdb.service.edass.generated.model.APIFilter;
import org.veupathdb.service.edass.generated.model.APIFilterImpl;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilter;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilterImpl;
import org.veupathdb.service.edass.generated.model.APIStringSetFilter;
import org.veupathdb.service.edass.generated.model.APIStringSetFilterImpl;
import org.veupathdb.service.edass.model.Entity;
import org.veupathdb.service.edass.model.EntityResultSetUtils;
import org.veupathdb.service.edass.model.TestModel;

public class StudiesTest {

  private static TestModel model;
  private static DataSource datasource;
  
  @BeforeAll
  public static void setUp() {
    model = new TestModel();
    datasource = Resources.getApplicationDataSource();
    
  }
  
  @Test
  @DisplayName("Test reading of entity table") 
  void testReadEntityTable() {
    String sql = "select e.*, n.name from entity e, entityname n " + 
        " where e.entity_name_id = n.entity_name_id and entity_id = 'GEMS-Part'";
    
    Entity entity = new SQLRunner(datasource, sql).executeQuery(rs -> {
      rs.next();
      Entity e = EntityResultSetUtils.createEntityFromResultSet(rs);
      return e;
    });
    assertEquals("GEMS-Part", entity.getId());
    assertEquals("Participants in the study", entity.getDescription());
    assertEquals( "Participant", entity.getName());
  }
  
  @Test
  @DisplayName("Test valid construction of filters from API filters")
  void testConstructFilters() {
    
    List<APIFilter> afs = new ArrayList<APIFilter>();
    
    APIStringSetFilter stringFilter = new APIStringSetFilterImpl();
    stringFilter.setEntityId(model.participant.getId());
    stringFilter.setVariableId(model.shoesize.getId());
    
    afs.add(stringFilter);
    
    APINumberRangeFilter numberFilter = new APINumberRangeFilterImpl();
    numberFilter.setEntityId(model.observation.getId());
    numberFilter.setVariableId(model.weight.getId());
    afs.add(numberFilter);
    
    Studies.constructFiltersFromAPIFilters(model.study, afs);
    
    assertEquals(2, afs.size());
  }

  @Test
  @DisplayName("Test rejection of invalid API filters")
  void testIncorrectConstructFilters() {
    
    assertThrows(InternalServerErrorException.class, new Executable() {
      
      @Override
      public void execute() throws Throwable {
        List<APIFilter> afs = new ArrayList<APIFilter>();
        
        // a legit filter
        APIStringSetFilter stringFilter = new APIStringSetFilterImpl();
        stringFilter.setEntityId(model.participant.getId());
        stringFilter.setVariableId(model.shoesize.getId());
        
        afs.add(stringFilter);
       
        // illegit... can't be the superclass
        APIFilter nakedFilter = new APIFilterImpl();
        nakedFilter.setEntityId(model.observation.getId());
        nakedFilter.setVariableId(model.weight.getId());
        afs.add(nakedFilter);

        Studies.constructFiltersFromAPIFilters(model.study, afs);
      }
    });
  }
  

}
