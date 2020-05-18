package m2sql;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

        public Object[] toRow(final int[] projects) {
            final List<Object> row = new ArrayList<>();
            for (final int fieldIndex: projects) {
                switch (fieldIndex) {
                    case 0:
                        row.add(this.getUid());
                        break;
                    case 1:
                        row.add(this.getGroupId());
                        break;
                    case 2:
                        row.add(this.getArtifactId());
                        break;
                }
            }
            return row.toArray();
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

        public Object[] toRow(final int[] projects) {
            final List<Object> row = new ArrayList<>();
            for (final int fieldIndex: projects) {
                switch (fieldIndex) {
                    case 0:
                        row.add(this.getUid());
                        break;
                    case 1:
                        row.add(this.getVersion());
                        break;
                    case 2:
                        row.add(this.getFilesize());
                        break;
                    case 3:
                        row.add(this.getLastModified());
                        break;
                    case 4:
                        row.add(this.getSha1());
                        break;
                }
            }
            return row.toArray();
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
        final String groupId = groupFolder.toString().substring(baseFolder.length()-1);
        return new Artifact(groupId.replace(File.separator, ".").substring(1), withoutVersion);
    }

    // xxx(okachaiev): i absolutely need to find a better way to present column names & indices
    // to avoid "switch" statements and magical .contains calls
    private ArtifactVersion pomPathToArtifactVersion(final int[] projects, final Path filePath) {
        final Set<Integer> fieldIndex = new HashSet<>();
        for (final int project: projects) {
            fieldIndex.add(project);
        }

        final Artifact artifact = pomPathToArtifact(filePath);
        final String filename = filePath.getFileName().toString();
        final String version = filename.substring(filename.lastIndexOf("-")+1, filename.lastIndexOf("."));

        final String fullPath = filePath.toString();
        final String jarPath = fullPath.substring(0, fullPath.lastIndexOf(".")) + JAR_EXTENSION;
        
        File jarFile = null;
        if (fieldIndex.contains(2) || fieldIndex.contains(3)) {
            jarFile = new File(jarPath);
        }

        long filesize = 0;
        if (fieldIndex.contains(2)) {
            filesize = jarFile.length();
        }

        long lastModified = 0;
        if (fieldIndex.contains(3)) {
            lastModified = jarFile.lastModified();
        }

        final String jarSHAPath = jarPath + JAR_SHA_EXTENSION;
        String sha = "";
        if (fieldIndex.contains(4)) {
            try {
                sha = FileUtils.readFileToString(new File(jarSHAPath), StandardCharsets.UTF_8);
            } catch (IOException e) {
                // no-op
            }
        }

        return new ArtifactVersion(artifact.getUid(), version, filesize, lastModified, sha);
    }

    public Stream<Artifact> findAll(String folderName) throws IOException {
        Path folderPath;
        if (null != folderName) {
            folderPath = Paths.get(baseFolderPath.toString(), folderName.replace(".", File.pathSeparator));
        } else {
            folderPath = baseFolderPath;
        }
        return Files.walk(folderPath)
                .filter(path -> path.toString().endsWith(POM_EXTENSION))
                .map(this::pomPathToArtifact)
                .distinct();
    }

	public Stream<ArtifactVersion> findAllVersions(final int[] projects) throws IOException {
        return Files.walk(baseFolderPath)
                .filter(path -> path.toString().endsWith(POM_EXTENSION))
                .map(path -> pomPathToArtifactVersion(projects, path))
                .distinct();
	}

}