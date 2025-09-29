import React, { useState, useEffect } from "react";

const UploadPage = () => {
  const [file, setFile] = useState(null);
  const [message, setMessage] = useState("");
  const [details, setDetails] = useState({
    inferred_types: [],
    sample_row_numbers: [],
    schema_query: null,
    inserted_rows: [],
    metadata: {},
    duplicates: [],
    has_duplicates: false,
  });
  const [loading, setLoading] = useState(false);
  const [expandedSection, setExpandedSection] = useState(null);
  const [batchFileIds, setBatchFileIds] = useState([]);
  const [selectedBatchId, setSelectedBatchId] = useState(null);
  const [batchData, setBatchData] = useState(null);
  const [showConfirmDialog, setShowConfirmDialog] = useState(false);
  const [step, setStep] = useState(0);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
    setMessage("");
    setDetails({
      inferred_types: [],
      sample_row_numbers: [],
      schema_query: null,
      inserted_rows: [],
      metadata: {},
      duplicates: [],
      has_duplicates: false,
    });
    setStep(0);
    setShowConfirmDialog(false);
  };

  const handleUpload = async () => {
    if (!file) {
      setMessage("Please select a file");
      return;
    }

    setLoading(true);
    setMessage("");

    try {
      // Step 1: Infer data types
      setMessage("Inferring data types...");
      const formData1 = new FormData();
      formData1.append("file", file);
      const res1 = await fetch("http://localhost:8000/infer-types/", {
        method: "POST",
        body: formData1,
      });
      if (!res1.ok) throw new Error(`HTTP error! status: ${res1.status}`);
      const data1 = await res1.json();
      setDetails((prev) => ({ ...prev, inferred_types: data1.inferred_types }));
      setStep(1);

      // Step 2: Sample rows
      setMessage("Sampling rows...");
      const formData2 = new FormData();
      formData2.append("file", file);
      const res2 = await fetch("http://localhost:8000/sample-rows/", {
        method: "POST",
        body: formData2,
      });
      if (!res2.ok) throw new Error(`HTTP error! status: ${res2.status}`);
      const data2 = await res2.json();
      setDetails((prev) => ({ ...prev, sample_row_numbers: data2.sample_row_numbers }));
      setStep(2);

      // Step 3: Generate schema
      setMessage("Generating schema...");
      const formData3 = new FormData();
      formData3.append("file", file);
      const res3 = await fetch("http://localhost:8000/generate-schema/", {
        method: "POST",
        body: formData3,
      });
      if (!res3.ok) throw new Error(`HTTP error! status: ${res3.status}`);
      const data3 = await res3.json();
      setDetails((prev) => ({ ...prev, schema_query: data3.schema_query, target_table: data3.target_table }));
      setStep(3);

      // Step 4: Check duplicates
      setMessage("Checking for duplicates...");
      const formData4 = new FormData();
      formData4.append("file", file);
      const res4 = await fetch("http://localhost:8000/check-duplicates/", {
        method: "POST",
        body: formData4,
      });
      if (!res4.ok) throw new Error(`HTTP error! status: ${res4.status}`);
      const data4 = await res4.json();
      setDetails((prev) => ({ ...prev, duplicates: data4.duplicates, has_duplicates: data4.has_duplicates }));
      setMessage(data4.message);
      
      if (data4.has_duplicates) {
        setShowConfirmDialog(true);
        setStep(4);
      } else {
        // Step 5: Insert rows
        await proceedWithInsert(true);
      }
    } catch (err) {
      setMessage("Upload failed: " + err.message);
      setStep(0);
    } finally {
      setLoading(false);
    }
  };

  const proceedWithInsert = async (proceed) => {
    try {
      setMessage("Inserting rows...");
      const formData5 = new FormData();
      formData5.append("file", file);
      const res5 = await fetch(`http://localhost:8000/confirm-insert/?proceed=${proceed}`, {
        method: "POST",
        body: formData5,
      });
      if (!res5.ok) throw new Error(`HTTP error! status: ${res5.status}`);
      const data5 = await res5.json();
      setDetails((prev) => ({
        ...prev,
        inserted_rows: data5.row_count > 0 ? prev.inserted_rows : [],
        metadata: data5.row_count > 0 ? {
          table_name: data5.table_name,
          file_id: data5.file_id,
          batch_id: data5.batch_id,
          run_id: data5.run_id,
          row_count: data5.row_count,
        } : {},
      }));
      setMessage(data5.message);
      setStep(5);
      setShowConfirmDialog(false);
    } catch (err) {
      setMessage("Upload failed: " + err.message);
      setStep(0);
    } finally {
      setLoading(false);
    }
  };

  const fetchBatchFileIds = async () => {
    try {
      const res = await fetch("http://localhost:8000/get-batch-file-ids/");
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      const data = await res.json();
      setBatchFileIds(data.batch_file_ids);
    } catch (err) {
      setMessage("Failed to fetch batch and file IDs: " + err.message);
    }
  };

  const handlePreviewBatch = async (batchId) => {
    try {
      const res = await fetch(`http://localhost:8000/preview-batch-data/${batchId}`);
      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
      const data = await res.json();
      setBatchData(data);
      setSelectedBatchId(batchId);
    } catch (err) {
      setMessage("Failed to preview batch data: " + err.message);
    }
  };

  const handleDeleteBatch = async (batchId) => {
    if (window.confirm(`Are you sure you want to delete data for batch_id ${batchId}?`)) {
      try {
        const res = await fetch(`http://localhost:8000/delete-batch-data/${batchId}`, {
          method: "DELETE",
        });
        if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
        const data = await res.json();
        setMessage(data.message);
        setBatchData(null);
        setSelectedBatchId(null);
        fetchBatchFileIds();
      } catch (err) {
        setMessage("Failed to delete batch data: " + err.message);
      }
    }
  };

  useEffect(() => {
    fetchBatchFileIds();
  }, []);

  const toggleSection = (section) => {
    setExpandedSection(expandedSection === section ? null : section);
  };

  return (
    <div className="min-h-screen bg-gray-100 p-6 md:p-8 flex">
      {/* Sidebar */}
      <div className="w-150 bg-white rounded-lg shadow-lg p-4 mr-6">
        <h2 className="text-xl font-bold text-gray-800 mb-4">Batch & File IDs</h2>
        <ul className="space-y-2">
          {batchFileIds.map(({ batch_id, file_id }) => (
            <li key={batch_id} className="flex items-center justify-between">
              <span
                className={`cursor-pointer text-blue-500 hover:underline ${selectedBatchId === batch_id ? 'font-bold' : ''}`}
                onClick={() => handlePreviewBatch(batch_id)}
              >
                Batch {batch_id} (File {file_id})
              </span>
              <button
                onClick={() => handleDeleteBatch(batch_id)}
                className="text-red-500 hover:text-red-700"
              >
                Delete
              </button>
            </li>
          ))}
        </ul>
        {batchData && (
          <div className="mt-4">
            <h3 className="text-lg font-semibold text-gray-700">Batch {selectedBatchId} Data</h3>
            <div className="p-4 bg-gray-50 overflow-x-auto">
              <table className="min-w-full text-sm text-gray-700">
                <thead>
                  <tr className="bg-gray-100">
                    {batchData.rows.length > 0 &&
                      Object.keys(batchData.rows[0]).map((key) => (
                        <th key={key} className="p-2 border-b">{key}</th>
                      ))}
                  </tr>
                </thead>
                <tbody>
                  {batchData.rows.map((row, index) => (
                    <tr key={index} className="border-b">
                      {Object.values(row).map((value, i) => (
                        <td key={i} className="p-2">{value}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>

      {/* Main Content */}
      <div className="flex-1 max-w-4xl bg-white rounded-lg shadow-lg p-6">
        <h1 className="text-3xl font-bold text-gray-800 mb-6">Upload CSV</h1>
        
        {/* File Input and Upload Button */}
        <div className="flex flex-col md:flex-row gap-4 mb-6">
          <input
            type="file"
            accept=".csv"
            onChange={handleFileChange}
            className="flex-1 border border-gray-300 rounded-md p-2 text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          <button
            onClick={handleUpload}
            disabled={loading}
            className={`bg-blue-500 text-white px-6 py-2 rounded-md hover:bg-blue-600 transition disabled:bg-blue-300 ${loading ? 'cursor-not-allowed' : ''}`}
          >
            {loading ? (
              <span className="flex items-center">
                <svg className="animate-spin h-5 w-5 mr-2" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8h8a8 8 0 01-8 8 8 8 0 01-8-8z" />
                </svg>
                Processing...
              </span>
            ) : (
              "Upload"
            )}
          </button>
        </div>

        {/* Message Display */}
        {message && (
          <p
            className={`text-sm mb-6 ${
              message.includes("failed")
                ? "text-red-500"
                : message.includes("No new rows inserted") || message.includes("canceled")
                ? "text-yellow-500"
                : "text-green-500"
            }`}
          >
            {message}
          </p>
        )}

        {/* Confirmation Dialog for Duplicates */}
        {showConfirmDialog && details.has_duplicates && (
          <div className="fixed inset-0 bg-gray-600 bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-6 max-w-2xl w-full">
              <h2 className="text-xl font-bold text-gray-800 mb-4">{details.message}</h2>
              <p className="text-gray-700 mb-4">Preview of duplicate rows (up to 5):</p>
              <div className="overflow-x-auto mb-4">
                <table className="min-w-full text-sm text-gray-700">
                  <thead>
                    <tr className="bg-gray-100">
                      {details.duplicates.length > 0 &&
                        Object.keys(details.duplicates[0]).map((key) => (
                          <th key={key} className="p-2 border-b">{key}</th>
                        ))}
                    </tr>
                  </thead>
                  <tbody>
                    {details.duplicates.map((row, index) => (
                      <tr key={index} className="border-b">
                        {Object.values(row).map((value, i) => (
                          <td key={i} className="p-2">{value}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p className="text-gray-700 mb-4">Proceed with inserting unique rows?</p>
              <div className="flex gap-4">
                <button
                  onClick={() => proceedWithInsert(true)}
                  className="bg-green-500 text-white px-4 py-2 rounded-md hover:bg-green-600"
                >
                  Yes
                </button>
                <button
                  onClick={() => proceedWithInsert(false)}
                  className="bg-red-500 text-white px-4 py-2 rounded-md hover:bg-red-600"
                >
                  No
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Details Display */}
        {details && (
          <div className="space-y-4">
            {/* Inferred Data Types */}
            {step >= 1 && (
              <div className="border rounded-md">
                <button
                  onClick={() => toggleSection("inferred_types")}
                  className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
                >
                  Inferred Data Types
                </button>
                {expandedSection === "inferred_types" && (
                  <div className="p-4 bg-gray-50">
                    <ul className="list-disc list-inside text-gray-700">
                      {details.inferred_types.map(([col, type], index) => (
                        <li key={index}>
                          <span className="font-medium">{col}</span>: {type}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}

            {/* Sample Row Numbers */}
            {step >= 2 && (
              <div className="border rounded-md">
                <button
                  onClick={() => toggleSection("sample_rows")}
                  className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
                >
                  Sample Row Numbers
                </button>
                {expandedSection === "sample_rows" && (
                  <div className="p-4 bg-gray-50">
                    <p className="text-gray-700">
                      Rows selected: {details.sample_row_numbers.join(", ") || "None"}
                    </p>
                  </div>
                )}
              </div>
            )}

            {/* Generated Schema */}
            {step >= 3 && (
              <div className="border rounded-md">
                <button
                  onClick={() => toggleSection("schema")}
                  className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
                >
                  Generated Schema
                </button>
                {expandedSection === "schema" && (
                  <div className="p-4 bg-gray-50">
                    <pre className="text-sm text-gray-700 whitespace-pre-wrap">
                      {details.schema_query || "Schema matched with existing table, no new schema generated."}
                    </pre>
                  </div>
                )}
              </div>
            )}

            {/* Inserted Rows */}
            {step >= 5 && details.inserted_rows.length > 0 && (
              <div className="border rounded-md">
                <button
                  onClick={() => toggleSection("inserted_rows")}
                  className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
                >
                  Inserted Rows (Sample)
                </button>
                {expandedSection === "inserted_rows" && (
                  <div className="p-4 bg-gray-50 overflow-x-auto">
                    <table className="min-w-full text-sm text-gray-700">
                      <thead>
                        <tr className="bg-gray-100">
                          {details.inserted_rows.length > 0 &&
                            Object.keys(details.inserted_rows[0]).map((key) => (
                              <th key={key} className="p-2 border-b">{key}</th>
                            ))}
                        </tr>
                      </thead>
                      <tbody>
                        {details.inserted_rows.map((row, index) => (
                          <tr key={index} className="border-b">
                            {Object.values(row).map((value, i) => (
                              <td key={i} className="p-2">{value}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            {/* Metadata Information */}
            {step >= 5 && Object.keys(details.metadata).length > 0 && (
              <div className="border rounded-md">
                <button
                  onClick={() => toggleSection("metadata")}
                  className="w-full text-left bg-gray-200 p-3 font-semibold text-gray-700 rounded-t-md hover:bg-gray-300"
                >
                  Metadata Information
                </button>
                {expandedSection === "metadata" && (
                  <div className="p-4 bg-gray-50">
                    <ul className="list-disc list-inside text-gray-700">
                      {Object.entries(details.metadata).map(([key, value]) => (
                        <li key={key}>
                          <span className="font-medium">{key}</span>: {value}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default UploadPage;
