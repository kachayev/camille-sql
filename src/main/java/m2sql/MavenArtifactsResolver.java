package m2sql;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import org.apache.commons.io.FileUtils;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class MavenArtifactsResolver {

    private static final String POM_EXTENSION = ".pom";

    private static final String JAR_EXTENSION = ".jar";

    private static final String JAR_SHA_EXTENSION = ".sha1";

    @Data
    @AllArgsConstructor
    public static class Artifact {
        private final String groupId;
        private final String artifactId;

        public long getUid() {
            final CRC32 crc = new CRC32();
            crc.update(this.toString().getBytes());
            return crc.getValue();
        }

        public String toString() {
            return String.format("%s::%s", groupId, artifactId);
        }

        public Object[] toRow() {
            final Object[] row = {
                this.getUid(),
                this.getGroupId(),
                this.getArtifactId()
            };
            return row;
        }
    }

    @Data
    @AllArgsConstructor
    public static class ArtifactVersion {
        private final long uid;
        private final String version;
        private final long filesize;
        private final long lastModified;
        private final String sha1;

        public Object[] toRow() {
            final Object[] row = {
                this.uid,
                this.version,
                this.filesize,
                this.lastModified,
                this.sha1,
            };
            return row;
        }
    }

    private final String baseFolder;

    private final Path baseFolderPath;

    public MavenArtifactsResolver(String baseFolder) {
        this.baseFolder = baseFolder;
        this.baseFolderPath = Path.of(baseFolder);
    }

    private Artifact pomPathToArtifact(Path filePath) {
        final String filename = filePath.getFileName().toString();
        final String withoutExtension = filename.substring(0, filename.lastIndexOf("."));
        final String withoutVersion = withoutExtension.substring(0, withoutExtension.lastIndexOf("-"));
        final Path groupFolder = filePath.getParent().getParent().getParent().toAbsolutePath();
        final String groupId = groupFolder.toString().substring(baseFolder.length());
        return new Artifact(groupId.replace(File.separator, ".").substring(1), withoutVersion);
    }

    private ArtifactVersion pomPathToArtifactVersion(Path filePath) {
        final Artifact artifact = pomPathToArtifact(filePath);
        final String filename = filePath.getFileName().toString();
        final String version = filename.substring(filename.lastIndexOf("-")+1, filename.lastIndexOf("."));

        final String fullPath = filePath.toString();
        final String jarPath = fullPath.substring(0, fullPath.lastIndexOf(".")) + JAR_EXTENSION;
        final File jarFile = new File(jarPath);
        final long filesize = jarFile.length();

        final String jarSHAPath = jarPath + JAR_SHA_EXTENSION;
        String sha = "";
        try {
            sha = FileUtils.readFileToString(new File(jarSHAPath), StandardCharsets.UTF_8);
        } catch (IOException e) {
            // no-op
        }

        return new ArtifactVersion(artifact.getUid(), version, filesize, jarFile.lastModified(), sha);
    }

    public Stream<Artifact> findAll() throws IOException {
        return Files.walk(baseFolderPath)
                .filter(path -> path.toString().endsWith(POM_EXTENSION))
                .map(this::pomPathToArtifact)
                .distinct();
    }

	public Stream<ArtifactVersion> findAllVersions() throws IOException {
        return Files.walk(baseFolderPath)
                .filter(path -> path.toString().endsWith(POM_EXTENSION))
                .map(this::pomPathToArtifactVersion)
                .distinct();
	}

}