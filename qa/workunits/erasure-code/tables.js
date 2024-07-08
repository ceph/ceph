$(function() {
    if (typeof encode_table != 'undefined') {
        let table_rows = '';
        for (let row of encode_table) {
            table_rows += `<tr>`
            for (let cell of row)
            {
                table_rows += `<td>${cell}</td>`
            }
            table_rows += `</tr>`;
            console.log(table_rows);
        }
        $('#encode-table').append(table_rows);
    }

    if (typeof decode_table != 'undefined') {
        let table_rows = '';
        for (let row of decode_table) {
            table_rows += `<tr>`
            for (let cell of row)
            {
                table_rows += `<td>${cell}</td>`
            }
            table_rows += `</tr>`;
        }
        $('#decode-table').append(table_rows);
    }
});