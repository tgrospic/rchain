package coop.rchain.casper.genesis.contracts

import coop.rchain.models.NormalizerEnv
import coop.rchain.rholang.build.CompiledRholangSource

final class RevGenerator private (supply: Long, code: String)
    extends CompiledRholangSource(code, NormalizerEnv.Empty) {
  val path: String = "<synthetic in Rev.scala>"
}

object RevGenerator {

  def apply(userVaults: Seq[Vault], supply: Long): RevGenerator = {
    val vaultBalanceList =
      userVaults.map(v => s"""("${v.revAddress.toBase58}", ${v.initialBalance})""").mkString(", ")

    val code: String =
      s""" new rl(`rho:registry:lookup`), revVaultCh in {
         #   rl!(`rho:rchain:revVault`, *revVaultCh) |
         #   for (@(_, RevVault) <- revVaultCh) {
         #     new revVaultInitCh in {
         #       @RevVault!("init", *revVaultInitCh) |
         #       for (TreeHashMap, @vaultMap, initVault <- revVaultInitCh) {
         #         match [$vaultBalanceList] {
         #           vaults => {
         #             new iter in {
         #               contract iter(@[(addr, initialBalance) ... tail]) = {
         #                  iter!(tail) |
         #                  new vault in {
         #                    initVault!(*vault, addr, initialBalance) |
         #                    TreeHashMap!("set", vaultMap, addr, *vault, Nil)
         #                  }
         #               } |
         #               iter!(vaults)
         #             }
         #           }
         #         }
         #       }
         #     }
         #   }
         # }
     """.stripMargin('#')

//    println(s"CODE: $code")
    new RevGenerator(supply, code)
  }
}
