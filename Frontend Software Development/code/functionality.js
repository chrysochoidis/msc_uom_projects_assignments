'use strict';

var allRows = [];
var rowsPerPage = 20;

/**
 * Upload csv file to browser
 */
function uploadCSV() {
    var fileUpload = document.getElementById("fileUpload");
    var regex = /^([a-zA-Z0-9\s_\\.\-:])+(.csv)$/;
    if (regex.test(fileUpload.value.toLowerCase())) {
        if (typeof (FileReader) != "undefined") {
            var reader = new FileReader();

            reader.onload = function (e) {

                //splits the file to make rows
                allRows = e.target.result.split("\n");

                //add Export Button
                var inputArea = document.getElementById("inputfileArea");
                var exportButton = document.createElement("button");
                exportButton.id = "exportButton";
                exportButton.innerHTML = "Export";
                inputArea.appendChild(exportButton);

                var addRecordTable = document.getElementById("addRecordTable");
                addRecordTable.style.display = "table";

                var dataTable = document.getElementById("dataTable");
                dataTable.style.display = "table";
                
                displayPage(1);
            }
            reader.readAsText(fileUpload.files[0]);
        } else {
            alert("Browser doesnt support HTML5.");
        }
    } else {
        alert("Please insert a CSV file.");
    }
}

/**
 * Transforms the csv data into a table data and splits by 20. The set of 20 rows that will be displayed, is determined by the number of page 
 * we are in. The first page is 1
 * @param {int} pageNumber - the desired page number 
 */
function displayPage(pageNumber) {

    //clears dataTable so that it displays the proper data from paging() 
    var dataTable = document.getElementById("dataTable");
    while (dataTable.rows.length > 1) {
        dataTable.deleteRow(1);
    }

    //determine the range of rows for the current page----index changes from page to page
    var startIndex = (pageNumber - 1) * rowsPerPage + 1; // +1 to skip the header row
    var endIndex = startIndex + rowsPerPage;
    
    for (let i = startIndex; i < endIndex && i < allRows.length; i++) {
        var cells = allRows[i].split(",");
        if (cells.length > 1) {
            var row = dataTable.insertRow(-1);
            for (var j = 0; j < cells.length; j++) {
                var cell = row.insertCell(-1);
                cell.innerHTML =  cells[j];
            }

            var actionCell = row.insertCell(-1);
            var editButton = document.createElement("button");
            editButton.id = "editRecord";
            editButton.innerHTML = "Edit";
            editButton.onclick = function (){ editRecord(this, i);};
            actionCell.appendChild(editButton);

        }
    }

    var exportButton = document.getElementById("exportButton");
    exportButton.onclick = tableToCSV;
    
    createPaging(pageNumber);
}

/**
 * Creates pagination element and buttons. Calls the displayPage function according to the button pressed
 * @param {int} currentPage 
 */
function createPaging(currentPage){
    var totalPages = Math.ceil(allRows.length / rowsPerPage);
    var dataArea = document.getElementById("dataArea");

    //clear the old pagination
    var oldPagination = document.getElementById("paging");
    if (oldPagination) {
        dataArea.removeChild(oldPagination);
    }

    //create pagination element
    var pagination = document.createElement("p");            
    pagination.id= "paging";
    pagination.style.display = "flex";
    pagination.style.justifyContent = "space around";

    //create prev button if page displayed > 1
    if (currentPage > 1) {
        var prevButton = document.createElement("button");
        prevButton.innerHTML = "Previous";
        prevButton.onclick = function() { displayPage(currentPage - 1); };
        prevButton.style = "margin: 5px;"
        pagination.appendChild(prevButton);
    }

    //create next button if page displayed < total pages 
    if (currentPage < totalPages) {
        var nextButton = document.createElement("button");
        nextButton.innerHTML = "Next";
        nextButton.onclick = function() { displayPage(currentPage + 1); };
        nextButton.style = "margin: 5px;"
        pagination.appendChild(nextButton);
    }

    dataArea.appendChild(pagination);
}

/**
 * Converts the cells of the row that contains the button parameter, to input element & change the button functionality to Save
 * exclunding the header row
 * @param {*} button - the particular button element that was clicked
 * @param {*} rowIndex - the row index that contains the button clicked. passed for future saving
 */
function editRecord(button, rowIndex){ 
    var chosenRow = button.parentNode.parentNode;
    var cellsToEdit = chosenRow.getElementsByTagName("td");

    cellsToEdit[0].innerHTML =  "<input type='text' value='" + cellsToEdit[0].innerText + "' />";
    cellsToEdit[1].innerHTML =  "<input type='text' value='" + cellsToEdit[1].innerText + "' />";
    cellsToEdit[2].innerHTML =  "<input list='genders' value='" + cellsToEdit[2].innerText + "' />";
    cellsToEdit[3].innerHTML =  "<input type='number' value='" + cellsToEdit[3].innerText + "' />";
    cellsToEdit[4].innerHTML =  "<input type='text' value='" + cellsToEdit[4].innerText + "' />";
    cellsToEdit[5].innerHTML =  "<input type='number' value='" + cellsToEdit[5].innerText + "' />";
    cellsToEdit[6].innerHTML =  "<input type='number' value='" + cellsToEdit[6].innerText + "' />";
    cellsToEdit[7].innerHTML =  "<input list='payments' value='" + cellsToEdit[7].innerText + "' />";
    cellsToEdit[8].innerHTML =  "<input type='text' value='" + cellsToEdit[8].innerText + "' />";
    cellsToEdit[9].innerHTML =  "<input type='text' value='" + cellsToEdit[9].innerText + "' />";

    button.innerHTML = "Save";
    button.onclick = function() { saveRecord(this, rowIndex); };
}

/**
 * After validating, save the cells of the row that contains the button parameter and display them as plain text, excluding the header row
 * @param {*} button - the particular button element that was clicked
 * @param {*} rowIndex - the row index that contains the button clicked
 * @returns  - false, just to stop the function if validation fails
 */
function saveRecord(button, rowIndex) {
    var chosenRow = button.parentNode.parentNode;
    var cellsToSave = chosenRow.getElementsByTagName("td");

    if (!validation(cellsToSave)) {
        return;
    }

    var updatedRowData = [];

    for (let i = 0; i < cellsToSave.length - 1; i++) { 
        var inputField = cellsToSave[i].getElementsByTagName("input")[0];
        var updatedValue = inputField.value;
        updatedRowData.push(updatedValue);
        cellsToSave[i].innerText = updatedValue;
    }

    allRows[rowIndex] = updatedRowData.join(",");
    button.innerHTML = "Edit";
    button.onclick = function() { editRecord(this); };
}

/**
 * Add the cell values of the addRecordTable to the dataTable after validating. After the addition, displays the first page
 * @returns - false, just to stop the function if validation fails
 */
function addRecord(){
    var addRecordRow = document.getElementById("addRecordRow");
    var cellsToAdd = addRecordRow.getElementsByTagName("td");

    if (!validation(cellsToAdd)) {
        return;
    }

    var rowToAdd = [];

    for(let i = 0; i < cellsToAdd.length -1; i++){
        var inputFields = cellsToAdd[i].getElementsByTagName("input")[0];
        var cellsToAddValues= inputFields.value;
        rowToAdd.push(cellsToAddValues);
        cellsToAdd[i].getElementsByTagName("input")[0].value = ""; 
    }

    allRows[allRows.length] = rowToAdd.join(",");
    displayPage(1);
}

//validation rules for addding record and saving an edited record
/**
 * 
 * @param {*} cells - the cells that need validation. Can be the addRecordTable or a certain row of the dataTable
 * @returns - false, if the validation failes, true, if validation succeeds
 */
function validation(cells){
    var errors = [];

    var invoice_no = cells[0].getElementsByTagName("input")[0].value.trim();
    var customer_id = cells[1].getElementsByTagName("input")[0].value.trim();
    var gender = cells[2].getElementsByTagName("input")[0].value;
    var age = cells[3].getElementsByTagName("input")[0].value.trim();
    var category = cells[4].getElementsByTagName("input")[0].value.trim();
    var quantity = cells[5].getElementsByTagName("input")[0].value.trim();
    var price = cells[6].getElementsByTagName("input")[0].value.trim();
    var payment_method = cells[7].getElementsByTagName("input")[0].value;
    var invoice_date = cells[8].getElementsByTagName("input")[0].value.trim();
    var shopping_mall = cells[9].getElementsByTagName("input")[0].value.trim();

    if (!invoice_no) errors.push("Invoice No is required");
    if (!customer_id) errors.push("Customer ID is required");
    if (!gender) errors.push("Gender must be selected");
    if (!age || isNaN(age) || age < 1 || age > 120) errors.push("Age must be a positive number");
    if (!category) errors.push("Category is required");
    if (!quantity || isNaN(quantity) || quantity <= 0) errors.push("Quantity must be a positive number");
    if (!price || isNaN(price) || price <= 0) errors.push("Price must be a positive number.");
    if (!payment_method) errors.push("Payment Method must be selected");
    if (!invoice_date) errors.push("Invoice Date is required");
    if (!shopping_mall) errors.push("Shopping Mall is required");

     // show the errors
     if (errors.length > 0) {
        showErrors(errors);
        return false; 
    } else {
        clearErrors();
    }

    return true; 
}

/**
 * Displays the error element for 5 seconds then clears
 * @param {*} errors - the error array
 */
function showErrors(errors){

    var errorArea = document.getElementById("errorArea");
    var errorMessages = document.getElementById("errors");
    errorMessages.innerHTML = errors.join("<br>");
    errorArea.style.display = "flex";

    setTimeout(()=> {
        clearErrors();
    }, 5000);
}

/**
 * Hides the error element
 */
function clearErrors(){
    var errorArea = document.getElementById("errorArea");
    var errorMessages = document.getElementById("errors");
    errorMessages.innerHTML = "";
    errorArea.style.display = "none";
}

/**
 * Converts allRows table to a csv type array. Not to be confused with the dataTable variable
 */
function tableToCSV(){
    var csv = [];

    for(let i = 0; i < allRows.length; i++){
        let rowData = allRows[i].trim();
        if(rowData !== ""){
        csv.push(rowData);
        }
    }

    csv = csv.join("\n");
    exportCSV(csv);
}

/**
 * Makes a temp link and "autoclicks" it to download the csv file 
 * @param {*} csv - the csv which has all the data stored
 */
function exportCSV(csv){
    
    var utf8BOM = "\uFEFF" + csv;

    var CSVFile = new Blob([utf8BOM], {type : "text/csv;charset=utf-8;"});
    var temp_link = document.createElement('a');

    temp_link.download = "data.csv";
    var url = window.URL.createObjectURL(CSVFile);
    temp_link.href = url;

    temp_link.style.display="none";
    document.body.appendChild(temp_link);

    temp_link.click();
    document.body.removeChild(temp_link);
}