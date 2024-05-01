import React, { useEffect, useState } from 'react'
import { Box } from '@mui/material'
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import Collapse from '@mui/material/Collapse';

import {
    createBrowserRouter,
    RouterProvider,
    useNavigate,
    useParams
} from "react-router-dom";
import { JSONTree } from 'react-json-tree'

const TasksList = () => {
    const navigate = useNavigate();
    const [tasks, setTasks] = useState([]);

    useEffect(() => {
        fetch('/api/tasks')
        .then(response => response.json())
        .then(tasks => setTasks(tasks));
    }, []);

    return (
        <Box display='flex' justifyContent='center' alignItems='center' flexDirection='column'>
            <h1>Tasks</h1>
            <TableContainer sx={{ width: '70vw' }} component={Paper}>
                <Table aria-label="simple table">
                    <TableHead>
                        <TableRow>
                            <TableCell>Id</TableCell>
                            <TableCell align="right">Status</TableCell>
                            <TableCell align="right">Stacktrace</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                    {tasks.map((task) => (
                        <TableRow
                            key={task.id}
                            sx={{ '&:last-child td, &:last-child th': { border: 0 }, '&:hover': { backgroundColor: '#e0e0e0' } }}
                            onClick={() => navigate(`/tasks/${task.id}`)}
                        >
                            <TableCell component="th" scope="row">{task.id}</TableCell>
                            <TableCell align="right">{task.status}</TableCell>
                            <TableCell align="right" sx={{ maxWidth: '100px' }}>{task.stacktrace}</TableCell>
                        </TableRow>
                    ))}
                    </TableBody>
                </Table>
            </TableContainer>
        </Box>
    );
};

const JSONTreeTheme = {
    scheme: 'ocean',
    author: 'chris kempson (http://chriskempson.com)',
    base00: 'white',
    base01: '#343d46',
    base02: '#4f5b66',
    base03: '#65737e',
    base04: '#a7adba',
    base05: '#c0c5ce',
    base06: '#dfe1e8',
    base07: '#eff1f5',
    base08: '#bf616a',
    base09: '#d08770',
    base0A: '#ebcb8b',
    base0B: '#a3be8c',
    base0C: '#96b5b4',
    base0D: '#8fa1b3',
    base0E: '#b48ead',
    base0F: '#ab7967'
};

const TaskViewTableRow = ({ object }) => {
    const [open, setOpen] = useState(false);
    const [json, setJson] = useState('');

    useEffect(() => {
        if (open && !json)
            fetch(`/api/storage/${object.path}`)
            .then(response => response.json())
            .then(json => setJson(json));
            // .then(json => setJson(JSON.stringify(json)));
    }, [open]);

    return (
        <>
            <TableRow
                key={object.id}
                sx={{ '&:last-child td, &:last-child th': { border: 0 }, '&:hover': { backgroundColor: '#e0e0e0' } }}
            >
                <TableCell
                    component="th"
                    onClick={ () => setOpen(!open) }
                    sx={{ userSelect: 'none' }}
                >
                    Open
                </TableCell>
                <TableCell component="th" scope="row">{object.id}</TableCell>
                <TableCell align="right">{object.path}</TableCell>
                <TableCell align="right">{object.algorithm_name}</TableCell>
            </TableRow>
            <TableRow>
                <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={6}>
                    <Collapse in={open} timeout="auto" unmountOnExit>
                        <strong>JSON</strong>
                        <JSONTree data={json} theme={JSONTreeTheme} />
                    </Collapse>
                </TableCell>
            </TableRow>
        </>
    );
};

const TaskView = () => {
    const { taskId } = useParams();
    const [task, setTask] = useState(null);
    const [objects, setObjects] = useState([]);

    useEffect(() => {
        fetch(`/api/tasks/${taskId}`)
        .then(response => response.json())
        .then(task => setTask(task));

        fetch(`/api/objects?task_id=${taskId}`)
        .then(response => response.json())
        .then(objects => setObjects(objects));
    }, []);

    return (
        <Box display='flex' justifyContent='center' alignItems='center' flexDirection='column'>
            <h1>Task {taskId}</h1>
                {
                    task ?
                    <>
                        <div>{task.status}</div>
                        <JSONTree data={task.inputs} theme={JSONTreeTheme} />
                        <div>{task.stacktrace}</div>
                    </>
                    : null
                }
            <h1>Objects</h1>
            <TableContainer sx={{ width: '70vw' }} component={Paper}>
                <Table>
                    <TableHead>
                        <TableRow>
                            <TableCell />
                            <TableCell>Id</TableCell>
                            <TableCell align="right">Path</TableCell>
                            <TableCell align="right">Algorithm name</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                    {
                        objects.map((object) => (
                            <TaskViewTableRow key={object.id} object={object} />
                        ))
                    }
                    </TableBody>
                </Table>
            </TableContainer>
        </Box>
    );
};

const router = createBrowserRouter([
    {
        path: "/tasks/:taskId",
        element: <TaskView />,
    },
    {
        path: "*",
        element: <TasksList />
    },
]);


function App() {

    return (
        <RouterProvider router={router} />
    );
}

export default App;