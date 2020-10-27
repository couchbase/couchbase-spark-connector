import sbtassembly.MergeStrategy

import java.io.File

class ShadedMergeStrategy extends MergeStrategy{
  override def name: String = "Add `shaded` to file."

  override def apply(temporaryDirectory: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] = {
    Right(files.map(file => file -> ("shaded." + file.getName)))
  }
}
