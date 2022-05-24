
plugins {
  `java-library`
  `maven-publish`
}

java {
  targetCompatibility = JavaVersion.VERSION_15
  sourceCompatibility = JavaVersion.VERSION_15
}

// Project settings
group   = "org.veupathdb.eda"
version = "1.0.0"

tasks.register("print-version") { print(version) }

repositories {
  mavenCentral()
  maven {
    name = "GitHubPackages"
    url  = uri("https://maven.pkg.github.com/veupathdb/maven-packages")
    credentials {
      username = project.findProperty("gpr.user") as String? ?: System.getenv("GITHUB_USERNAME")
      password = project.findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
    }
  }
}

java {
  withSourcesJar()
  withJavadocJar()
}

tasks.jar {
  manifest {
    attributes["Implementation-Title"]   = project.name
    attributes["Implementation-Version"] = project.version
  }
}

val test by tasks.getting(Test::class) {
  // Use junit platform for unit tests
  useJUnitPlatform()
}

publishing {
  repositories {
    maven {
      name = "GitHub"
      url  = uri("https://maven.pkg.github.com/veupathdb/maven-packages")
      credentials {
        username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
        password = project.findProperty("gpr.key") as String? ?: System.getenv("TOKEN")
      }
    }
  }
  publications {
    create<MavenPublication>("gpr") {
      from(components["java"])
      pom {
        name.set("EDA Subsetting Library")
        description.set("Provides Java interface to query and provide EDA data and metadata from a database")
        url.set("https://github.com/VEuPathDB/lib-eda-subsetting")
        developers {
          developer {
            id.set("ryanrdoherty")
            name.set("Ryan Doherty")
            email.set("rdoherty@upenn.edu")
            url.set("https://github.com/ryanrdoherty")
            organization.set("VEuPathDB")
          }
        }
        scm {
          connection.set("scm:git:git://github.com/VEuPathDB/lib-eda-subsetting.git")
          developerConnection.set("scm:git:ssh://github.com/VEuPathDB/lib-eda-subsetting.git")
          url.set("https://github.com/VEuPathDB/lib-eda-subsetting")
        }
      }
    }
  }
}

// // // // // // // // // // // // // // // // // // // // // // // // // //
//
// Project Dependencies
//
// // // // // // // // // // // // // // // // // // // // // // // // // //

val jackson = "2.13.3"      // FasterXML Jackson version
val jersey  = "3.0.4"       // Jersey/JaxRS version
val junit   = "5.7.2"       // JUnit version
val log4j   = "2.17.2"      // Log4J version
val fgputil = "2.5-jakarta" // FgpUtil version

val implementation      by configurations
val testImplementation  by configurations
val runtimeOnly         by configurations
val annotationProcessor by configurations
val testRuntimeOnly     by configurations

dependencies {

  // FgpUtil Dependencies
  implementation("org.gusdb:fgputil-core:${fgputil}")
  implementation("org.gusdb:fgputil-db:${fgputil}")
  implementation("org.gusdb:fgputil-json:${fgputil}")
  implementation("org.gusdb:fgputil-web:${fgputil}")

  // Jersey
  implementation("org.glassfish.jersey.containers:jersey-container-grizzly2-http:${jersey}")
  implementation("org.glassfish.jersey.containers:jersey-container-grizzly2-servlet:${jersey}")
  implementation("org.glassfish.jersey.media:jersey-media-json-jackson:${jersey}")
  runtimeOnly("org.glassfish.jersey.inject:jersey-hk2:${jersey}")

  implementation("org.glassfish.jersey.core:jersey-client:3.0.3")

  // Jackson
  implementation("com.fasterxml.jackson.core:jackson-databind:${jackson}")
  implementation("com.fasterxml.jackson.core:jackson-annotations:${jackson}")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${jackson}")

  // Log4J
  implementation("org.apache.logging.log4j:log4j-api:${log4j}")
  implementation("org.apache.logging.log4j:log4j-core:${log4j}")
  implementation("org.apache.logging.log4j:log4j:${log4j}")

  // Stub database (temporary?)
  implementation("org.hsqldb:hsqldb:2.5.1")

  // Unit Testing
  testImplementation("org.junit.jupiter:junit-jupiter:${junit}")
  testImplementation("org.mockito:mockito-core:2.+")
}
