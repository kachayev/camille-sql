package m2sql;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class MavenArtifactsResolver {

    private static final String POM_EXTENSION = ".pom";

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
        return new Artifact(groupId.replace(File.separator, "."), withoutVersion);
    }

    public Stream<Artifact> findAll() throws IOException {
        return Files.walk(baseFolderPath)
                .filter(path -> path.toString().endsWith(POM_EXTENSION))
                .map(this::pomPathToArtifact)
                .distinct();
    }

}