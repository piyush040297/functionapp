package com.function;

import com.microsoft.azure.functions.*;
import org.junit.jupiter.api.Test;

import java.util.logging.Logger;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit test for BlobToSqlFunction class.
 */
public class BlobToSqlFunctionTest {

    @Test
    public void testBlobTriggerFunction() throws Exception {
        // Mock blob data as byte array
        byte[] mockBlobData = "name,age\nJohn,30\nAlice,25".getBytes();

        // Mock blob name
        String blobName = "test.csv";

        // Mock ExecutionContext
        ExecutionContext context = mock(ExecutionContext.class);
        doReturn(Logger.getGlobal()).when(context).getLogger();

        // Instantiate function and invoke run method
        BlobToSqlFunction function = new BlobToSqlFunction();
        function.run(mockBlobData, blobName, context);

        // Assertions can be added based on expected database interactions or logs
        assertNotNull(function);
    }
}
