package backchat

import java.io.File
import org.apache.commons.lang.SystemUtils
import com.eaio.uuid.UUID

object FileUtils {

  private val MAX_TMP_DIR_TRIES = 5
  /**
   * Returns a new empty temporary directory.
   */
  def createTempDir(): File = {
    var tempDir: File = null
    var tries = 0
    do {
      // For sanity sake, die eventually if we keep failing to pick a new unique directory name.
      tries += 1
      if (tries > MAX_TMP_DIR_TRIES) {
        throw new IllegalStateException("Failed to create a new temp directory in "
          + MAX_TMP_DIR_TRIES + " attempts, giving up");
      }
      tempDir = new File(SystemUtils.getJavaIoTmpDir, new UUID().toString);
    } while (!tempDir.mkdir());
    tempDir
  }
}