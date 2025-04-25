package com.multiavrostreamconsumer.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.multiavrostreamconsumer.configuration.DBConfig;
import com.multiavrostreamconsumer.utility.AzureTokenService;

@Service
public class KeyVaultService {
    private static final Logger logger = LogManager.getLogger(DBConfig.class);
    @Autowired
    private AzureTokenService azureTokenService;

    @Value("${keyvault-name}")
    private String keyVaultName;

//   private String keyVaultName = "";

    private static final String KEY_VAULT_URL_TEMPLATE = "https://%s.vault.azure.net/secrets/%s?api-version=2016-10-01";

    public String getSecret(String secretName) {
        try {
            String accessToken = azureTokenService.getAccessToken();
            String url = String.format(KEY_VAULT_URL_TEMPLATE, keyVaultName, secretName);

            CloseableHttpClient client = HttpClients.createDefault();
            HttpGet request = new HttpGet(url);
            request.addHeader("Authorization", "Bearer " + accessToken);

            HttpResponse response = client.execute(request);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            // Extract secret value from the response
            String secretValue = parseSecretValue(result.toString());
            client.close();
            return secretValue;
        } catch (Exception e) {
            logger.info("getSecret method secretName: " + secretName + " and errorMessage: " + e.getMessage());
        }
        return "";
    }

    private String parseSecretValue(String responseBody) throws IOException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(responseBody);
            return jsonNode.get("value").asText();
        } catch (Exception e) {
            logger.info("get secreat method secretName" + responseBody + " error Message :" + e.getMessage());
        }
        return "";
    }
}
