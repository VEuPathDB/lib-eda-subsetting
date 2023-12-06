package org.veupathdb.service.eda.ss.model.variable.binary;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.gusdb.fgputil.json.JsonUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.BinaryProperties;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public class BinaryFilesManager {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  final StudyFinder studyFinder;

  public static final String ANCESTORS_FILE_NAME = "ancestors";
  public static final String IDS_MAP_FILE_NAME = "ids_map";
  private static final String STUDY_FILE_PREFIX = "study_";
  private static final String ENTITY_FILE_PREFIX = "entity_";
  private static final String VAR_FILE_PREFIX = "var_";
  private static final String VOCAB_FILE_PREFIX = "vocab_";
  private static final String UTF_8_POSTFIX = "_utf8";

  public static final String META_KEY_BYTES_PER_ANCESTOR = "bytesReservedPerAncestor";

  static final String DONE_FILE_NAME = "DONE";

  public enum Operation { READ, WRITE };

  private static final Logger LOG = LogManager.getLogger(BinaryFilesManager.class);

  /*
 studies/
  study_GEMS1A/
    DONE                 # empty file indicating study dump completed successfully
    entity_EUPA_12345/   # an entity ID
      ancestors          # ancestor ID indices.
      ids_map            # index -> ID map
      meta.json          # simple info about the files
      var_EUPA_44444     # a variable file
      var_EUPA_55555     # another variable file
      vocab_EUPA_55555   # a vocabulary file
   *
   */

  public BinaryFilesManager(Path studiesDirectory) {
    this.studyFinder = new SimpleStudyFinder(studiesDirectory.toString());
  }

  public BinaryFilesManager(StudyFinder studyFinder) {
    this.studyFinder = studyFinder;
  }

  public boolean studyHasFiles(Study study) {
    return studyHasFiles(study.getInternalAbbrev());
  }

  public boolean studyHasFiles(String studyAbbrev) {
    final Optional<Path> studyDir = getStudyDirIfExists(studyAbbrev);
    if (studyDir.isEmpty()) {
      LOG.debug("Looked for study dir for study {} but could not find it.", studyAbbrev);
      return false;
    }
    if (!getDoneFile(studyDir.get(), Operation.READ).toFile().exists()) {
      LOG.debug("Study directory for study {} exists but data is incomplete.", studyAbbrev);
      return false;
    }
    LOG.info("Looked for study dir for study {} and found it. studyHasFiles() will return true.", studyAbbrev);
    return true;
  }


  public Path getStudyDir(Study study, Operation op) {
    if (op == Operation.READ) {
      return mustGetExistingStudyDir(study.getInternalAbbrev());
    }
    else {
      Path studyDir = studyFinder.findStudyPath(getStudyDirName(study.getInternalAbbrev()));
      createDir(studyDir);
      return studyDir;
    }
  }

  public boolean studyDirExists(Study study) {
    return getStudyDirIfExists(study.getInternalAbbrev()).isPresent();
  }

  public Path getEntityDir(Study study, Entity entity, Operation op) {
    if (op == Operation.READ) return getEntityDir(study.getInternalAbbrev(), entity);
    else {
      Path entityDir = Path.of(studyFinder.findStudyPath(getStudyDirName(study.getInternalAbbrev())).toString(), getEntityDirName(entity));
      createDir(entityDir);
      return entityDir;
    }
  }

  public boolean entityDirExists(Study study, Entity entity) {
    return Files.isDirectory(Path.of(mustGetExistingStudyDir(study.getInternalAbbrev()).toString(), getEntityDirName(entity)));
  }

  public Path getAncestorFile(Study study, Entity entity, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, ANCESTORS_FILE_NAME);
    return createFile(study.getInternalAbbrev(), entity, ANCESTORS_FILE_NAME);
  }

  public boolean ancestorFileExists(Study study, Entity entity) {
    return Files.exists(Path.of(getStudyDir(study, Operation.READ).toString(), getEntityDirName(entity), ANCESTORS_FILE_NAME));
  }

  public Path getIdMapFile(Study study, Entity entity, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, IDS_MAP_FILE_NAME);
    return createFile(study.getInternalAbbrev(), entity, IDS_MAP_FILE_NAME);
  }

  public boolean idMapFileExists(Study study, Entity entity) {
    return Files.exists(Path.of(getStudyDir(study, Operation.READ).toString(), getEntityDirName(entity), IDS_MAP_FILE_NAME));
  }

  public Path getVariableFile(Study study, Entity entity, Variable var, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, getVarFileName(var));
    return createFile(study.getInternalAbbrev(), entity, getVarFileName(var));
  }

  public Path getUtf8VariableFile(Study study, Entity entity, Variable var, Operation op) {
    final String fileName = getVarFileName(var) + UTF_8_POSTFIX;
    if (op == Operation.READ) return getFile(study, entity, fileName);
    return createFile(study.getInternalAbbrev(), entity, fileName);
  }

  public boolean variableFileExists(Study study, Entity entity, Variable var) {
    return Files.exists(Path.of(getStudyDir(study, Operation.READ).toString(), getEntityDirName(entity), getVarFileName(var)));
  }

  Path getVocabFile(Study study, Entity entity, Variable var, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, getVocabFileName(var));
    return createFile(study.getInternalAbbrev(), entity, getVocabFileName(var));
  }

  public Path getMetaJsonFile(Study study, Entity entity, Operation op) {
    if (op == Operation.READ) return getFile(study, entity, "meta.json");
    return createFile(study.getInternalAbbrev(), entity, "meta.json");
  }

  public Path getMetaJsonFile(String studyAbbrev, Entity entity, Operation op) {
    if (op == Operation.READ) return getFile(studyAbbrev, entity, "meta.json");
    return createFile(studyAbbrev, entity, "meta.json");
  }

  public Path getDoneFile(Path directory, Operation op) {
    if (op == Operation.WRITE) return createDoneFile(directory);

    Path filepath = Path.of(directory.toString(), DONE_FILE_NAME);
    if (!Files.exists(filepath)) throw new RuntimeException("File '" + filepath + "' does not exist");
    return filepath;
  }

  public Long getDataVersionUsed(Study study) {
    final Optional<Path> studyDir = getStudyDirIfExists(study.getInternalAbbrev());
    try {
      return JsonUtil.Jackson.readValue(getDoneFile(studyDir.orElseThrow(), Operation.READ).toFile(), DoneMetadata.class).getDataVersion();
    } catch (IOException e) {
      LOG.warn("Unable to find data version in DONE file, returning null.", e);
      return null;
    }
  }

  /**
   * For a given entity, parse the metadata JSON file to determine the number of bytes needed to store identifiers
   * for all the entity's ancestors.
   * @return a list of ancestors in order of closest to the farthest ascendant.
   */
  public List<Integer> getBytesReservedForAncestry(Study study, Entity entity) {
    JSONObject metajson = readMetaJsonFile(getMetaJsonFile(study, entity, Operation.READ));
    List<Integer> bytesReservedPerAncestors = new ArrayList<>();
    JSONArray ancestorBytesReserved = metajson.getJSONArray(META_KEY_BYTES_PER_ANCESTOR);
    for (int i = 0; i < ancestorBytesReserved.length(); i++) {
      bytesReservedPerAncestors.add(ancestorBytesReserved.getInt(i));
    }
    return bytesReservedPerAncestors;
  }

  /**
   * For a given entity, parse the metadata JSON file to determine the number of bytes needed to store identifiers
   * for the entity.
   * @return number of bytes reserved for entity.
   */
  public Integer getBytesReservedForEntity(Study study, Entity entity) {
    return readMetadata(study.getInternalAbbrev(), entity).get().getBytesReservedForId();
  }

  public void writeMetadata(Study study, Entity entity, Metadata metadata) {
    try {
      OBJECT_MAPPER.writeValue(getMetaJsonFile(study, entity, Operation.WRITE).toFile(), metadata);
    } catch (IOException e) {
      throw new RuntimeException("Failed writing meta.json file.", e);
    }
  }

  /**
   * Returns empty if files are not available in this environment.
   * @param studyAbbrev
   * @param entity
   * @return
   */
  public Optional<Metadata> readMetadata(String studyAbbrev, Entity entity) {
    try {
      File file = getMetaJsonFile(studyAbbrev, entity, Operation.READ).toFile();
      if (!file.exists()) {
        return Optional.empty();
      }
      return Optional.of(OBJECT_MAPPER.readValue(file, Metadata.class));
    } catch (IOException e) {
      throw new RuntimeException("Failed reading meta.json file.", e);
    }
  }

  ////////////////////////////////////////////////////////

  private String getStudyDirName(String studyAbbrev) {
    return STUDY_FILE_PREFIX + studyAbbrev;
  }

  private String getEntityDirName(Entity entity) {
    return ENTITY_FILE_PREFIX + entity.getId();
  }

  private String getVarFileName(Variable var) {
    return VAR_FILE_PREFIX + var.getId();
  }

  private String getVocabFileName(Variable var) {
    return VOCAB_FILE_PREFIX + var.getId();
  }

  private static JSONObject readMetaJsonFile(Path metajsonFile) {
    if (!metajsonFile.toFile().exists()) throw new RuntimeException("metajson file '" + metajsonFile + "' does not exist");
    try {
      String jsonString = Files.readString(metajsonFile);
      JSONObject json = new JSONObject(jsonString);
      return json;
    } catch (IOException | JSONException e) {
      throw new RuntimeException("Failed reading meta json file", e);
    }
  }

  private Path createFile(String studyAbbrev, Entity entity, String filename) {

    Path entityDir = getEntityDir(studyAbbrev, entity);
    Path filepath = Path.of(entityDir.toString(), filename);
    LOG.info("Creating file: " + filepath);
    try {
      Files.createFile(filepath);
    } catch (FileAlreadyExistsException e) {
      throw new RuntimeException("Failed creating file '" + filepath + "'.  It already exists.", e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return filepath;
  }

  private Path getFile(Study study, Entity entity, String fileName) {
    Path entityDir = getEntityDir(study.getInternalAbbrev(), entity);
    Path file = Path.of(entityDir.toString(), fileName);
    if (!Files.exists(file)) throw new RuntimeException("File '" + file + "' does not exist");
    return file;
  }

  private Path getFile(String studyAbbrev, Entity entity, String fileName) {
    Path entityDir = getEntityDir(studyAbbrev, entity);
    Path file = Path.of(entityDir.toString(), fileName);
    if (!Files.exists(file)) throw new RuntimeException("File '" + file + "' does not exist");
    return file;
  }


  private void createDir(Path dir) {
    LOG.info("Creating dir: " + dir);
    try {
      Files.createDirectory(dir);
    } catch (FileAlreadyExistsException e) {
      throw new RuntimeException("Failed creating directory '" + dir + "'.  It already exists.", e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Path getEntityDir(String studyAbbrev, Entity entity) {
    Path entityDir = Path.of(mustGetExistingStudyDir(studyAbbrev).toString(),
        getEntityDirName(entity));
    if (!Files.isDirectory(entityDir)) throw new RuntimeException("Entity directory '" + entityDir + "' does not exist");
    return entityDir;
  }

  private Path mustGetExistingStudyDir(String studyAbbrev) {
    return getStudyDirIfExists(studyAbbrev)
        .orElseThrow(() -> new RuntimeException("Study directory '" + studyFinder.findStudyPath(getStudyDirName(studyAbbrev)) + "' does not exist"));
  }

  private Optional<Path> getStudyDirIfExists(String studyAbbrev) {
    Path studyDir = studyFinder.findStudyPath(getStudyDirName(studyAbbrev));
    LOG.debug("Looking for study dir " + studyDir.toString());
    if (!Files.isDirectory(studyDir)) return Optional.empty();
    return Optional.of(studyDir);
  }

  private Path createDoneFile(Path directory) {
    Path filepath = Path.of(directory.toString(), DONE_FILE_NAME);
    LOG.info("Creating file: " + filepath);
    try {
      Files.createFile(filepath);
    } catch (FileAlreadyExistsException e) {
      throw new RuntimeException("Failed creating file '" + filepath + "'.  It already exists.", e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return filepath;
  }

  public static class Metadata {
    private List<VariableMeta> variableMetadata;
    private List<Integer> bytesReservedPerAncestor;
    private Integer bytesReservedForId;

    public Metadata() { }

    public List<VariableMeta> getVariableMetadata() {
      return variableMetadata;
    }

    public void setVariableMetadata(List<VariableMeta> variableMetadata) {
      this.variableMetadata = variableMetadata;
    }

    public List<Integer> getBytesReservedPerAncestor() {
      return bytesReservedPerAncestor;
    }

    public void setBytesReservedPerAncestor(List<Integer> bytesReservedPerAncestor) {
      this.bytesReservedPerAncestor = bytesReservedPerAncestor;
    }

    public Integer getBytesReservedForId() {
      return bytesReservedForId;
    }

    public void setBytesReservedForId(Integer bytesReservedForId) {
      this.bytesReservedForId = bytesReservedForId;
    }
  }

  public static class VariableMeta {
    private String type;
    private String variableId;
    private BinaryProperties properties;

    public VariableMeta() {

    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getVariableId() {
      return variableId;
    }

    public void setVariableId(String variableId) {
      this.variableId = variableId;
    }

    public BinaryProperties getProperties() {
      return properties;
    }

    public void setProperties(BinaryProperties properties) {
      this.properties = properties;
    }
  }
}
