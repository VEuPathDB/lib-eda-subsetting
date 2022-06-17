
// // // // // // // // // // // // // // // // // // // // // // // // // //
//
// Project Configuration
//
// // // // // // // // // // // // // // // // // // // // // // // // // //

// Project settings
group   = "org.veupathdb.eda"
version = "1.5.0"

plugins {
  `java-library`
  `maven-publish`
}

java {
  targetCompatibility = JavaVersion.VERSION_11
  sourceCompatibility = JavaVersion.VERSION_11
  withSourcesJar()
  withJavadocJar()
}

val test by tasks.getting(Test::class) {
  // use junit platform for unit tests
  useJUnitPlatform {
    excludeTags = setOf("Performance")
  }
}

val perfTest = task<Test>("perfTest") {
  useJUnitPlatform {
    includeTags = setOf("Performance")
  }

  outputs.upToDateWhen { false } // Never cache results, we always want to actually run the perf test.

  description = "Runs integration tests."
  group = "verification"

  systemProperties = mapOf(
    "numFiles" to System.getProperty("numFiles"),
    "recordCount" to System.getProperty("recordCount"),
    "cached" to System.getProperty("cached")
  )
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

// Never cache changing dependencies
configurations.all {
  resolutionStrategy.cacheChangingModulesFor(0, TimeUnit.SECONDS)
}

repositories {
  mavenCentral()
  mavenLocal()
  maven {
    name = "GitHubPackages"
    url  = uri("https://maven.pkg.github.com/veupathdb/maven-packages")
    credentials {
      username = project.findProperty("gpr.user") as String? ?: System.getenv("GITHUB_USERNAME")
      password = project.findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
    }
  }
}

val fgputil = "2.5-jakarta" // FgpUtil version
val log4j   = "2.17.2"      // Log4J version
val junit   = "5.8.2"       // JUnit version

dependencies {

  // FgpUtil Dependencies
  implementation("org.gusdb:fgputil-core:${fgputil}")
  implementation("org.gusdb:fgputil-db:${fgputil}")
  implementation("org.gusdb:fgputil-json:${fgputil}")
  implementation("org.gusdb:fgputil-web:${fgputil}")

  // Log4J
  implementation("org.apache.logging.log4j:log4j-api:${log4j}")
  implementation("org.apache.logging.log4j:log4j-core:${log4j}")

  // Stub database (included in distribution since StubDB is used in EdaSubsettingService unit tests)
  implementation("org.hsqldb:hsqldb:2.6.1")

  // Unit Testing
  testImplementation("org.junit.jupiter:junit-jupiter:${junit}")
  testImplementation("org.hamcrest:hamcrest:2.2")
}
