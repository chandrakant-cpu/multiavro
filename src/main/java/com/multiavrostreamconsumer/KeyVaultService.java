package com.multiavrostreamconsumer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class KeyVaultService {

    @Value("${keyvault-name}")
    private String keyVaultName;

//   private String keyVaultName = "";

    public String getSecret(String secretName) {
        // TODO: Implement secret retrieval from Azure Key Vault
        return "";
    }
}
