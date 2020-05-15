package coop.rchain.casper

import com.google.protobuf.ByteString
import coop.rchain.casper.protocol._
import coop.rchain.crypto.codec._
import coop.rchain.crypto.signatures.Signed
import scodec.bits.ByteVector

object PrettyPrinter {

  def buildStringNoLimit(b: Array[Byte]): String = Base16.encode(b)
  def buildStringNoLimit(b: ByteString): String  = Base16.encode(b.toByteArray)

  def buildString(t: CasperMessage): String =
    t match {
      case b: BlockMessage => buildString(b)
      case _               => "Unknown consensus protocol message"
    }

  def bsStr(x: ByteString)        = ByteVector(x.toByteArray).toHex.take(10)
  def parentsStr(b: BlockMessage) = b.header.parentsHashList.map(x => s"${bsStr(x)}").mkString(", ")
  // TODO shouldn header.parentsHashList be nonempty list?
  private def buildString(b: BlockMessage): String =
    b.header.parentsHashList.headOption
      .fold(
        s"${buildString(b.blockHash)} parents: -"
      )(
        mainParent =>
          s"${buildString(b.blockHash)} " +
            //            s"-- Sender ID ${buildString(b.sender)} " +
            //            s"-- M Parent Hash ${buildString(mainParent)} " +
            s"parents: ${parentsStr(b)} "
        //            s"-- Contents ${buildString(b.body.state)}" +
        //            s"-- Shard ID ${limit(b.shardId, 10)}"
      )

  def buildString(bh: BlockHashMessage): String =
    s"Block hash: ${buildString(bh.blockHash)}"

  private def limit(str: String, maxLength: Int): String =
    if (str.length > maxLength) {
      str.substring(0, maxLength) + "..."
    } else {
      str
    }

  def buildString(d: ProcessedDeploy): String =
    s"User: ${buildStringNoLimit(d.deploy.pk.bytes)}, Cost: ${d.cost.toString} " +
      s"${buildString(d.deploy)}"

  def buildString(b: ByteString): String =
    limit(Base16.encode(b.toByteArray), 10)

  def buildStringSig(b: ByteString): String = {
    val bytes = b.toByteArray
    val str1  = Base16.encode(bytes.take(10))
    val str2  = Base16.encode(bytes.takeRight(10))
    s"${str1}...${str2}"
  }

  def buildString(sd: Signed[DeployData]): String =
    s"${buildString(sd.data)}, Sig: ${buildStringSig(sd.sig)}, SigAlgorithm: ${sd.sigAlgorithm.name}, ValidAfterBlockNumber: ${sd.data.validAfterBlockNumber}"

  def buildString(d: DeployData): String =
    s"DeployData #${d.timestamp} -- ${d.term}"

  def buildString(r: RChainState): String =
    buildString(r.postStateHash)

  def buildString(b: Bond): String =
    s"${buildStringNoLimit(b.validator)}: ${b.stake.toString}"
}
