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
      s""" new rl(`rho:registry:lookup`), listOpsCh, revVaultCh in {
         #   rl!(`rho:lang:listOps`, *listOpsCh) |
         #   rl!(`rho:rchain:revVault`, *revVaultCh) |
         #   for (@(_, RevVault) <- revVaultCh;
         #        @(_, ListOps)  <- listOpsCh) {
         #     new revVaultInitCh in {
         #       @RevVault!("init", *revVaultInitCh) |
         #       for (TreeHashMap, @vaultMap, initVault <- revVaultInitCh) {
         #         match [$vaultBalanceList] {
         #           vaults => {
         #             new createVaultCh in {
         #               @ListOps!("parMap", vaults, *createVaultCh, Nil) |
         #               contract createVaultCh(@(addr, initialBalance), @createdCh) = {
         #                 new vault in {
         #                   initVault!(*vault, addr, initialBalance) |
         #                   TreeHashMap!("set", vaultMap, addr, *vault, createdCh)
         #                 }
         #               }
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
