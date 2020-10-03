package org.veupathdb.service.edass.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.InternalServerErrorException;

import org.veupathdb.service.edass.generated.model.APIDateRangeFilter;
import org.veupathdb.service.edass.generated.model.APIDateRangeFilterImpl;
import org.veupathdb.service.edass.generated.model.APIFilter;
import org.veupathdb.service.edass.generated.model.APIFilterImpl;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilter;
import org.veupathdb.service.edass.generated.model.APINumberRangeFilterImpl;
import org.veupathdb.service.edass.generated.model.APIStringSetFilter;
import org.veupathdb.service.edass.generated.model.APIStringSetFilterImpl;
import org.veupathdb.service.edass.model.TestModel;

public class StudiesTest {

  private static TestModel model;
  
  @BeforeAll
  public static void setUp() {
    model = new TestModel();
  }

  @Test
  @DisplayName("Test valid construction of filters from API filters")
  void testConstructFilters() {
    
    Set<APIFilter> afs = new HashSet<APIFilter>();
    
    APIStringSetFilter stringFilter = new APIStringSetFilterImpl();
    stringFilter.setEntityId(model.participant.getEntityId());
    stringFilter.setVariableId(model.shoesize.getId());
    
    afs.add(stringFilter);
    
    APINumberRangeFilter numberFilter = new APINumberRangeFilterImpl();
    numberFilter.setEntityId(model.observation.getEntityId());
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
        Set<APIFilter> afs = new HashSet<APIFilter>();
        
        // a legit filter
        APIStringSetFilter stringFilter = new APIStringSetFilterImpl();
        stringFilter.setEntityId(model.participant.getEntityId());
        stringFilter.setVariableId(model.shoesize.getId());
        
        afs.add(stringFilter);
       
        // illegit... can't be the superclass
        APIFilter nakedFilter = new APIFilterImpl();
        nakedFilter.setEntityId(model.observation.getEntityId());
        nakedFilter.setVariableId(model.weight.getId());
        afs.add(nakedFilter);

        Studies.constructFiltersFromAPIFilters(model.study, afs);
      }
    });
  }
  

}
