const express = require("express");
const cors = require("cors");
const app = express();
const PORT = 3000;

app.use(
    cors({
        origin: ["http://localhost:5173"],
    })
);

// Route
app.get("/", (req, res) => {
    res.send("Hi this is the NODE SERVER!!!");
});

// Another Route 
app.get("/api/hello", (req, res) => {
    res.json({message: "Hi this FROM THE API!!!"});
});

app.listen(PORT, () => {
    console.log(`Server is runnong on http://localhost:${PORT}`);
});