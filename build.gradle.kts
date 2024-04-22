
// // // // // // // // // // // // // // // // // // // // // // // // // //
//
// Project Configuration
//
// // // // // // // // // // // // // // // // // // // // // // // // // //

// Project settings
group   = "org.veupathdb.eda"
version = "4.14.1"

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
tasks.withType<Jar> {
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

//tasks.named("jar") {
//  duplicatesStrategy =
//}

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
    url = uri("https://maven.pkg.github.com/veupathdb/maven-packages")
    credentials {
      username = project.findProperty("gpr.user") as String? ?: System.getenv("GITHUB_USERNAME")
      password = project.findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
    }
  }
}


dependencies {

  // FgpUtil Dependencies
  val fgputil = "2.12.16-jakarta"
  implementation("org.gusdb:fgputil-core:${fgputil}")
  implementation("org.gusdb:fgputil-db:${fgputil}")
  implementation("org.gusdb:fgputil-json:${fgputil}")
  implementation("org.gusdb:fgputil-web:${fgputil}")

  // Log4J
  implementation("org.apache.logging.log4j:log4j-api:2.20.0")
  implementation("org.apache.logging.log4j:log4j-core:2.20.0")

  // Stub database (included in distribution since StubDB is used in EdaSubsettingService unit tests)
  implementation("org.hsqldb:hsqldb:2.7.1")

  // Unit Testing
  testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
  testImplementation("org.mockito:mockito-core:5.2.0")
  testImplementation("org.hamcrest:hamcrest:2.2")
}
