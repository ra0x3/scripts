#!/bin/bash

pubkey="D4kbpNW4xm7vLGDySuwY95EgyfkaHo2NNd7Dda2f1gx3"
solana=/Users/ralston/.local/share/solana/install/active_release/bin/solana
for src in "https://api.testnet.solana.com" "https://api.devnet.solana.com" "localhost";
do
	echo "$solana airdrop 1 $pubkey --url $src"
	$solana airdrop 1 $pubkey --url $src
	sleep 5
done

